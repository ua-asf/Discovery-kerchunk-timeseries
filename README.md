# Discovery Kerchunk Timeseries Package

This package is a wrapper around [Kerchunk](https://github.com/fsspec/kerchunk) for generating
zarr stores for individual netcdf4/hdf5 files as well as consolidating spatially aligned zarr stores
into a single temporal zarr store.

## Installation

To install the latest stable version of the asf_kerchunk_timeseries package, run the following command:

``` bash
pip install git+https://github.com/ua-asf/Discovery-kerchunk-timeseries@stable
```

## Usage
### Kerchunk netcdf4 File

To generate a zarr store for a single netcdf4 file, run `generate_kerchunk_file_store()` with the uri of the target netcdf file

``` python
from asf_kerchunk_timeseries import generate_kerchunk_file_store

netcdf_uri = 's3://bucket-name/path/to/netcdf/file_00_version_v0.3.nc'
json_store_bytes = generate_kerchunk_file_store(netcdf_uri, netcdf_product_version='v0.3')

fsspec.open('s3://destination_file/for/zarr/store_00.json', 'wb') as f:
    f.write(json_store_bytes)
```
### Combine multiple netcdf4 Zarr Stores

To generate a zarr store for a single stack, use `generate_kerchunk_file_store_stack()`
with a list of the s3 uris for the temporal stack

``` python
from asf_kerchunk_timeseries import generate_kerchunk_file_store_stack

timestep_zarr_stores = ['s3://bucket-name/path/to/netcdf/file_00.json', ..., 's3://bucket-name/path/to/netcdf/file_01.json']
json_timeseries_store_bytes = generate_kerchunk_file_store_stack(timestep_zarr_stores)

fsspec.open('s3://destination_file/for/zarr/stack_00.json', 'wb') as f:
    f.write(json_timeseries_store_bytes)
```

### aiobotocore session
If credentials are needed to access the s3 bucket for the source netcdf4 data or zarr json stores, an aiobotocore session can be passed to `generate_kerchunk_file_store()`, and two separate aiobotocore sessions can be passed to `generate_kerchunk_file_store_stack()` (for cases where the source netcdf4 data and  zarr json stores are not in the same bucket and the environment doesn't have immediate permission to access both). Kerchunk will use these session(s) to read the s3 file(s).

``` python
# for single timestep
data = generate_kerchunk_file_store(netcdf_uri, 'vX.X', session=authenticated_aio_session)

# for stack
# if the environment doesn't have default permissions to read from the provided zarr uris,
# OR the netcdf4 data those zarr json stores are referencing,
# separate sessions can be provided for either bucket.
# (kerchunk will fallback to the `Default` profile in aws credentials file, then the current system if that doesn't exist)
stack_data = generate_kerchunk_file_store_stack(
    zarr_json_uris,
    netcdf4_bucket_session=session_with_netcdf4_bucket_permissions 
    zarr_bucket_session=session_with_zarr_store_bucket_permissions
)
```
--------