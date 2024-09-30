# Discovery Kerchunk Timeseries Package

This package is a wrapper around [Kerchunk](https://github.com/fsspec/kerchunk) for generating
zarr stores for individual netcdf4/hdf5 files as well as consolidating spatially aligned zarr stores
into a single temporal zarr store.

## Usage
### Kerchunk netcdf4 File

To generate a zarr store for a single netcdf4 file, run `generate_kerchunk_file_store()` with the uri of the target netcdf file

``` python
from .kerchunk_timeseries.kerchunk_netcdf4 import generate_kerchunk_file_store

netcdf_uri = 's3://bucket-name/path/to/netcdf/file_00.nc'
json_store_bytes = generate_kerchunk_file_store(netcdf_uri)

fsspec.open('s3://destination_file/for/zarr/store_00.json', 'wb') as f:
    f.write(json_store_bytes)
```
### Combine multiple netcdf4 Zarr Stores

To generate a zarr store for a single stack, use `generate_kerchunk_file_store_stack()`
with a list of the s3 uris for the temporal stack

``` python
from .kerchunk_timeseries.kerchunk_netcdf4 import generate_kerchunk_file_store_stack

timestep_zarr_stores = ['s3://bucket-name/path/to/netcdf/file_00.json', ..., 's3://bucket-name/path/to/netcdf/file_01.json']
json_timeseries_store_bytes = generate_kerchunk_file_store_stack(timestep_zarr_stores)

fsspec.open('s3://destination_file/for/zarr/stack_00.json', 'wb') as f:
    f.write(json_timeseries_store_bytes)
```

### aiobotocore session
If credentials are needed to access the s3 bucket, an aiobotocore session can be passed to `generate_kerchunk_file_store()` and `generate_kerchunk_file_store_stack()`. Kerchunk will use this session to read the s3 file(s).

``` python
generate_kerchunk_file_store(netcdf_uri, session=authenticated_aio_session)
 generate_kerchunk_file_store_stack(zarr_timestep_uris, session=authenticated_aio_session)
```
--------