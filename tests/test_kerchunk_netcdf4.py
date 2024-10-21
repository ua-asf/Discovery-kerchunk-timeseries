import json
import numpy as np
import pandas as pd
import xarray as xr

import os
from pathlib import Path
from unittest.mock import patch

from asf_kerchunk_timeseries import (
    generate_kerchunk_file_store,
    generate_kerchunk_file_store_stack,
)

from asf_kerchunk_timeseries.kerchunk_netcdf4 import _add_data_variable


def _generate_data(
    x_dim: int = 4,
    y_dim: int = 4,
    reference_datetime: np.datetime64 = np.datetime64("2022-11-07T00:00:00.000000000"),
    secondary_datetime: np.datetime64 = np.datetime64("2022-12-13T00:00:00.000000000"),
) -> tuple[xr.Dataset, xr.Dataset]:
    """Generates xarray datasets with x, y, time (secondary_datetime) dims,
    and two datavariable groups (unwrapped_phase & displacement)"""
    arr = np.random.randn(1, x_dim, y_dim)

    dims = ["time", "x", "y"]
    coords = {
        "time": [secondary_datetime],
        "x": list(range(x_dim)),
        "y": list(range(y_dim)),
    }

    data_vars = {}
    for var in ["unwrapped_phase", "displacement"]:
        data_vars[var] = xr.DataArray(data=arr, dims=dims, coords=coords)

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
    f = open(file, mode=mode, **kwargs)
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

        _write_to_file(file, dataset, identity)

        zarr_store = f"{file}.zarr"
        with open(zarr_store, "wb") as f:
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

        test_data = xr.open_dataset(zarr_store, engine="kerchunk")
        identification = xr.open_dataset(
            zarr_store, engine="kerchunk", group="identification"
        )

        assert test_data["bytes"] == 1000.0
        assert test_data["netcdf_uri"].item() == str(Path(file))
        assert test_data["source_file_name"].item() == Path(file).name
        assert test_data["product_version"].item() == "v0.0"
        assert (
            pd.Timestamp(identification["secondary_datetime"].item()).to_datetime64()
            == secondary_datetime
        )

    uris = [f"{file}.zarr" for file in files]
    zarr_stack_store = "test_frame.zarr"
    with open(zarr_stack_store, "wb") as f:
        f.write(json.dumps(generate_kerchunk_file_store_stack(uris, spec)).encode())

    stack_data = xr.open_dataset(zarr_stack_store, engine="kerchunk")
    assert "source_file_name" in stack_data.coords
    assert "x" in stack_data.coords
    assert "y" in stack_data.coords
    assert "time" not in stack_data.coords
    for file in files:
        assert file in stack_data["netcdf_uri"]
        assert file.split("/")[-1] in stack_data["source_file_name"]
        timestep_dataset = xr.open_dataset(f"{file}.zarr", engine="kerchunk")

        # ensure selectable by filename as expected
        timestep_in_stack = stack_data.sel(
            source_file_name=timestep_dataset["source_file_name"]
        )

        # we use slightly different coordinates in the single timestep and stack
        # (time in the single timestep vs source_file_name in the stack)

        for key in [
            "displacement",
            "unwrapped_phase",
        ]:
            assert (
                timestep_dataset[key]
                .drop_vars("time")
                .equals(timestep_in_stack[key].drop_vars("source_file_name"))
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
