import copy
from typing import Any, Optional
import numpy as np
import zarr
import ujson
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr, drop
from aiobotocore.session import AioSession
from s3fs import S3FileSystem
import h5py

def generate_kerchunk_file_store(
    netcdf_uri: str,
    netcdf_product_version: str,
    fsspec_options: dict = {
        "mode": "rb",
        "anon": False,
        "default_fill_cache": False,
        "default_cache_type": "first",
        "default_block_size": 1024 * 1024,
    },
    session: Optional[AioSession] = None,
) -> bytes:
    """
    Creates a zarr store for the provided netcdf/h5 file using the kerchunk library
    and returns it as bytes

    Parameters
    ----------
    netcdf_uri: str (Required)
        The S3 uri to build the zarr store from
    netcdf_product_version: str (Required)
        The product version of the source netcdf file
    fsspec_options: dict (Optional)
        options to pass to fsspec for opening the provided files
    session: AioSession (Optional)
        an AioBotocore session. Use this if the default AWS profile doesn't have read/write access
        to the provided uri. Ex:
            `session = aiobotocore.session.AioSession(profile='test_user')`

    Returns
    -------
    The encoded json zarr store for the provided `netcdf_uri` as bytes
    """
    s3 = S3FileSystem(session=session)

    with s3.open(netcdf_uri, **fsspec_options) as netcdf_infile:
        dataset = h5py.File(netcdf_infile, 'r')
        reference_datetime = dataset['identification']['reference_datetime'][()]
        secondary_datetime = dataset['identification']['secondary_datetime'][()]
        h5_chunks = SingleHdf5ToZarr(
            h5f=netcdf_infile, url=netcdf_uri, inline_threshold=300
        )

        file_size = netcdf_infile.size

        # store uri and size of file at time of writing zarr store
        zarr_store = zarr.hierarchy.open_group(h5_chunks.store)
        source_file_name = netcdf_uri.split('/')[-1]

        _add_data_variable(zarr_store, "netcdf_uri", data=netcdf_uri, dtype=str)
        _add_data_variable(zarr_store, "bytes", data=file_size, dtype=float)
        _add_data_variable(zarr_store, 'source_file_name', data=source_file_name, dtype=str)
        _add_data_variable(zarr_store, 'product_version', data=netcdf_product_version, dtype=str)
        _add_data_variable(
            zarr_store,
            'reference_datetime',
            data=reference_datetime,
            dtype=np.dtype('datetime64[ns]'),
        )
        _add_data_variable(
            zarr_store,
            'secondary_datetime',
            data=secondary_datetime,
            dtype=np.dtype('datetime64[ns]'),
        )

        return ujson.dumps(h5_chunks.translate()).encode()


def generate_kerchunk_file_store_stack(
    zarr_uris: list[str],
    fsspec_options: dict = {
        "mode": "rb",
        "anon": False,
        "default_fill_cache": False,
        "default_cache_type": "first",
        "default_block_size": 1024 * 1024,
    },
    session: Optional[AioSession] = None,
) -> bytes:
    """
    Creates a consolidated zarr store from a list of zarr json stores.
    concatenated along the "secondary_datetime" axis and returns the new zarr json store as bytes
    (note: drops the "time" axis as workaround for odd stacking behavior. Data is equivalent to "secondary_datetime")

    Parameters
    ----------
    zarr_uris: str (Required)
        The S3 zarr uri build the zarr store from
    fsspec_options: dict (Optional)
        options to pass to fsspec while opening the provided file uri
    session: AioSession (Optional)
        an AioBotocore session. Use this if the default AWS profile doesn't have read/write access
        to the provided uri. Ex:
            `session = aiobotocore.session.AioSession(profile='test_user')`

    Returns
    -------
    The encoded combined json zarr store for the provided `zarr_uris` as bytes
    """
    storage_options = copy.deepcopy(fsspec_options)
    storage_options["session"] = session

    drop_time = drop('time')
    zarr_chunks = MultiZarrToZarr(
        zarr_uris,
        target_options=fsspec_options,
        remote_protocol='s3',
        concat_dims=['source_file_name'],
        identical_dims=['y', 'x'],
        preprocess=drop_time,
    )
    multi_zarr_store = zarr_chunks.translate()

    return ujson.dumps(multi_zarr_store).encode()


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
