import json
import numpy as np
import pandas as pd
import xarray as xr

import os
import gzip
from pathlib import Path
from unittest.mock import patch
import fsspec
from asf_kerchunk_timeseries import (
    generate_kerchunk_file_store,
    generate_kerchunk_file_store_stack,
    filter_unused_references,
)

from asf_kerchunk_timeseries.kerchunk_netcdf4 import _add_data_variable


def _generate_data(
    x_dim: int = 4,
    y_dim: int = 4,
    reference_datetime: np.datetime64 = np.datetime64("2022-11-07T00:00:00.000000000"),
    secondary_datetime: np.datetime64 = np.datetime64("2022-12-13T00:00:00.000000000"),
) -> tuple[xr.Dataset, xr.Dataset]:
    """Generates xarray datasets with x, y, time (secondary_datetime) dims,
    and datavariable short_wavelength_displacement"""
    arr = np.random.randn(1, x_dim, y_dim)

    dims = ["time", "x", "y"]
    coords = {
        "time": [secondary_datetime],
        "x": list(range(x_dim)),
        "y": list(range(y_dim)),
    }

    data_vars = {}
    data_vars["short_wavelength_displacement"] = xr.DataArray(
        data=arr, dims=dims, coords=coords
    )

    dataset = xr.Dataset(data_vars=data_vars)

    identity = xr.Dataset(
        data_vars=dict(
            reference_datetime=reference_datetime,
            secondary_datetime=secondary_datetime,
        ),
        coords={"time": secondary_datetime},
    )

    return dataset, identity


def _write_to_file(file: str, dataset: xr.Dataset, identity: xr.Dataset):
    dataset.to_netcdf(file, group="/", format="NETCDF4")
    identity.to_netcdf(file, group="/identification", format="NETCDF4", mode="a")


def _open_with_size(file, mode: str, **kwargs):
    accepted = ["compression"]
    filtered_kwargs = {
        key: kwarg for key, kwarg in kwargs.items() if key.lower() in accepted
    }
    f = open(file, mode=mode, **filtered_kwargs)
    f.size = 1000.0
    return f


def _create_mock_bucket(bucket_name: str):
    bucket_path = f"tests/{bucket_name}"
    if not Path(bucket_path).exists():
        os.mkdir(f"tests/{bucket_name}")


# https://stackoverflow.com/a/68246809
@patch("s3fs.S3FileSystem.open", side_effect=_open_with_size)
@patch("s3fs.S3FileSystem.ls", side_effect=os.listdir)
def test_kerchunk_file_workflow(_mock_s3fs_ls, _mock_s3fs_open):
    bucket = "mock_bucket"
    _create_mock_bucket(bucket)

    size = 255
    files = [
        f"tests/{bucket}/test_data_{size}_{size}_{timestep}.nc"
        for timestep in range(0, 14)
    ]
    spec = {"mode": "rb"}
    fs = fsspec.filesystem("local")
    for offset, file in enumerate(files):
        reference_datetime = np.datetime64("2022-11-07T00:00:00.000000000")
        secondary_datetime = np.datetime64(
            "2022-12-13T00:00:00.000000000"
        ) + np.timedelta64(offset, "D")

        dataset, identity = _generate_data(
            255,
            255,
            reference_datetime=reference_datetime,
            secondary_datetime=secondary_datetime,
        )

        dataset["remove_me"] = [1, 2, 3]

        _write_to_file(file, dataset, identity)

        zarr_store = f"{file}.zarr.gz"
        # with fs.open(zarr_store, mode="wb", compression="gzip") as f:
        with gzip.open(zarr_store, "wb") as f:
            f.write(
                json.dumps(
                    generate_kerchunk_file_store(
                        file,
                        netcdf_product_version="v0.0",
                        final_netcdf_uri=file,
                        fsspec_options=spec,
                    )
                ).encode()
            )
        fs = fsspec.filesystem(
            "reference",
            fo=zarr_store,
            # One line required to specify compression here
            target_options={"compression": "gzip"},
        )
        mapper = fs.get_mapper("")
        test_data = xr.open_dataset(mapper, engine="zarr", consolidated=False)
        # test_data = xr.open_zarr(zarr_store, storage_options={'compression': 'gzip'}, consolidated=False)
        identification = xr.open_dataset(
            mapper, engine="zarr", group="identification", consolidated=False
        )

        assert test_data["bytes"] == 1000.0
        assert test_data["netcdf_uri"].item() == str(Path(file))
        assert test_data["source_file_name"].item() == Path(file).name
        assert test_data["product_version"].item() == "v0.0"
        assert (
            pd.Timestamp(identification["secondary_datetime"].item()).to_datetime64()
            == secondary_datetime
        )

    uris = [f"{file}.zarr.gz" for file in files]
    zarr_stack_store = "test_frame.zarr.gz"
    # spec["compression"] = "infer"

    mzz_steps = []
    for file in uris:
        with gzip.open(file, "rb") as new_f:
            zarr_store = json.loads(new_f.read().decode())
            filter_unused_references(zarr_store)
            mzz_steps.append(zarr_store)

    with gzip.open(zarr_stack_store, "wb") as f:
        consolidated = generate_kerchunk_file_store_stack(mzz_steps)
        f.write(json.dumps(consolidated).encode())

    fs = fsspec.filesystem(
        "reference",
        fo=zarr_stack_store,
        # One line required to specify compression here
        target_options={"compression": "gzip"},
    )
    stack_data = xr.open_zarr(fs.get_mapper(""), consolidated=False)
    assert "remove_me" not in stack_data.variables
    assert "source_file_name" in stack_data.coords
    assert "x" in stack_data.coords
    assert "y" in stack_data.coords
    assert "time" not in stack_data.coords
    for file in files:
        fs = fsspec.filesystem(
            "reference",
            fo=f"{file}.zarr.gz",
            # One line required to specify compression here
            target_options={"compression": "gzip"},
        )
        assert file in stack_data["netcdf_uri"]
        assert file.split("/")[-1] in stack_data["source_file_name"]
        timestep_dataset = xr.open_zarr(
            fs.get_mapper(""),
            consolidated=False,
        )

        # ensure selectable by filename as expected
        timestep_in_stack = stack_data.sel(
            source_file_name=timestep_dataset["source_file_name"]
        )

        # we use slightly different coordinates in the single timestep and stack
        # (time in the single timestep vs source_file_name in the stack)

        assert (
            timestep_dataset["short_wavelength_displacement"]
            .drop_vars("time")
            .equals(
                timestep_in_stack["short_wavelength_displacement"].drop_vars(
                    "source_file_name"
                )
            )
        )

        # additional variables added during processing lack source file's 'time' coordinate
        for key in [
            "bytes",
            "netcdf_uri",
            "source_file_name",
            "product_version",
            "reference_datetime",
            "secondary_datetime",
        ]:
            assert timestep_dataset[key].equals(
                timestep_in_stack[key].drop_vars("source_file_name")
            )


def test_add_data_variable():
    #  = zarr.group(zarr_version=1)
    data, _ = _generate_data(255, 255)
    zarr_data = data.to_zarr(zarr_version=1)
    test_zarr = zarr_data.zarr_group
    random_data = np.random.randn(10, 10)
    _add_data_variable(test_zarr, "test_zarr_group", random_data, float)

    for stored, original in zip(test_zarr["test_zarr_group"], random_data):
        assert original.tolist() == stored.tolist()
