from typing import Any, Optional, Union
import numpy as np
import zarr
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
from aiobotocore.session import AioSession
from s3fs import S3FileSystem
import h5py

# only fields with these prefixes will be kept
# improves stack time and memory read time
# for the variables the api will actually use
_fields_whitelist = [
    ".zattrs",
    ".zgroup",
    "bytes",
    "corrections",
    "identification",
    "metadata",
    "netcdf_uri",
    "product_version",
    "reference_datetime",
    "secondary_datetime",
    "short_wavelength_displacement",
    "source_file_name",
    "spatial_ref",
    "x",
    "y",
]

# these fields will pass the whitelist above but aren't guaranteed
# to be the same length (causing an error with kerchunk)
_fields_blacklist = [
    "metadata/reference_orbit/position",
    "metadata/reference_orbit/time",
    "metadata/reference_orbit/velocity",
    "metadata/secondary_orbit/position",
    "metadata/secondary_orbit/time",
    "metadata/secondary_orbit/velocity",
]


def generate_kerchunk_file_store(
    netcdf_uri: str,
    final_netcdf_uri: str,
    netcdf_product_version: str,
    fsspec_options: dict = {},
    session: Optional[AioSession] = None,
) -> dict[str, Any]:
    """
    Creates a zarr store for the provided netcdf/h5 file using the kerchunk library
    and returns it as a dictionary.

    Parameters
    ----------
    netcdf_uri: str (Required)
        The S3 uri to build the zarr store from
    final_netcdf_uri: str (Required)
        Will be written to the `netcdf_uri` and `source_file_name` data variables in the zarr store.
        This is where users should expect to find the final source netcdf data after any staging processes.
    netcdf_product_version: str (Required)
        The product version of the source netcdf file.
    fsspec_options: dict (Optional)
        options to pass to fsspec for opening the provided files.
    session: AioSession (Optional)
        an AioBotocore session. Used to access the netcdf4 data bucket.

        Use this if the default AWS profile doesn't have read/write access
        to the provided uri. Ex:
            `session = aiobotocore.session.AioSession(profile='test_user')`

    Returns
    -------
    The translated json zarr store for the provided `netcdf_uri` as bytes
    (see kerchunk docs for spec https://fsspec.github.io/kerchunk/spec.html#version-1)
    """
    options = {
        "mode": "rb",
        "anon": False,
        "default_fill_cache": False,
        "default_cache_type": "first",
        "default_block_size": 1024 * 1024,
        **fsspec_options,
    }

    s3 = S3FileSystem(session=session)

    with s3.open(netcdf_uri, **options) as netcdf_infile:
        dataset = h5py.File(netcdf_infile, "r")
        reference_datetime = dataset["identification"]["reference_datetime"][()]
        secondary_datetime = dataset["identification"]["secondary_datetime"][()]
        h5_chunks = SingleHdf5ToZarr(
            h5f=netcdf_infile, url=netcdf_uri, inline_threshold=300
        )

        file_size = netcdf_infile.size

        # store uri and size of file at time of writing zarr store
        zarr_store = zarr.hierarchy.open_group(h5_chunks.store)
        source_file_name = final_netcdf_uri.split("/")[-1]

        _add_data_variable(zarr_store, "netcdf_uri", data=final_netcdf_uri, dtype=str)
        _add_data_variable(zarr_store, "bytes", data=file_size, dtype=float)
        _add_data_variable(
            zarr_store, "source_file_name", data=source_file_name, dtype=str
        )
        _add_data_variable(
            zarr_store, "product_version", data=netcdf_product_version, dtype=str
        )
        _add_data_variable(
            zarr_store,
            "reference_datetime",
            data=reference_datetime,
            dtype=np.dtype("datetime64[ns]"),
        )
        _add_data_variable(
            zarr_store,
            "secondary_datetime",
            data=secondary_datetime,
            dtype=np.dtype("datetime64[ns]"),
        )

        return h5_chunks.translate()


def generate_kerchunk_file_store_stack(
    zarr_uris: Union[list[str], dict[str, Any]],
    target_opts: dict = {},
    remote_opts: dict = {},
) -> dict[str, Any]:
    """
    Creates a consolidated zarr store from a list of zarr json stores.
    concatenated along the "secondary_datetime" axis and returns the new zarr json store as bytes
    (note: drops the "time" axis as workaround for odd stacking behavior. Data is equivalent to "secondary_datetime")

    Parameters
    ----------
    zarr_uris: str (Required)
        The S3 zarr uris (or list of in-memory zarr store dicts) to build the consolidated zarr store from
    target_opts: dict
        options to pass to fsspec while opening the provided zarr uris.
        (passed to `MultiZarrToZarr`'s `target_options` keyword)
    remote_opts: dict
        options to pass to fsspec while opening the original netcdf data referenced by the zarr stores.
        (passed to `MultiZarrToZarr`'s `remote_options` keyword)
    Returns
    -------
    The consolidated json zarr store for the provided `zarr_uris` as a dict
    """
    defaults = {
        "anon": False,
        "default_fill_cache": False,
        "default_cache_type": "first",
        "default_block_size": 1024 * 1024,
    }
    target_options = {**defaults, **target_opts}

    remote_options = {**defaults, **remote_opts}

    key_fields_only = _drop_all_and_keep(
        fields_to_keep=_fields_whitelist,
        fields_to_drop=_fields_blacklist,
    )

    zarr_chunks = MultiZarrToZarr(
        zarr_uris,
        target_options=target_options,
        remote_options=remote_options,
        remote_protocol="s3",
        concat_dims=["source_file_name"],
        identical_dims=["y", "x"],
        preprocess=key_fields_only,
    )

    return zarr_chunks.translate()


def filter_unused_references(data: dict) -> None:
    """
    Modifies a zarr store dictionary to remove excess fields
    that aren't in _fields_whitelist, or are in _fields_blacklist
    """
    refs = data["refs"]
    for k in list(refs):
        keep = False
        for field in _fields_whitelist:
            if k.startswith(field):
                keep = True
                continue
        if not keep:
            refs.pop(k)
        for field in _fields_blacklist:
            if k.startswith(field):
                refs.pop(k)



def _add_data_variable(
    store: zarr.hierarchy.Group, key: str, data: Any, dtype: type
) -> None:
    """
    Inserts `data` of the given `dtype` at the given `key` in the provided zarr `store`.
    Adds '_ARRAY_DIMENSIONS' attribute for xarray deserialization

    Parameters
    ----------
    store: zarr.hierarchy.Group (required)
        The zarr group to insert the data
    key: str (required)
        The key that will be used for this new dataset variable
    data: any (required)
        The value to store in the new group
    dtype: type (required)
        The numpy dtype to use for the new group
    """
    store.create_dataset(key, data=data, dtype=dtype)

    # xarray won't be able to open the store without '_ARRAY_DIMENSIONS' attribute
    store[key].attrs["_ARRAY_DIMENSIONS"] = []


def _drop_all_and_keep(fields_to_keep: list[str], fields_to_drop: list[str]):
    """Generates preprocessor for removing ALL but the given list of fields, and drop the specific fields"""

    def preproc(refs):
        for k in list(refs):
            keep = False
            for field in fields_to_keep:
                if k.startswith(field):
                    keep = True
                    continue
            if not keep:
                refs.pop(k)
            for field in fields_to_drop:
                if k.startswith(field):
                    refs.pop(k)
        return refs

    return preproc
