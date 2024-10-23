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
import json

netcdf_uri = 's3://bucket-name/staging/path/to/netcdf/file_00_v0.3.nc'
final_netcdf_uri = 's3://bucket-name/final/path/to/netcdf/OPERA_DISP_file_00_v0.3.nc'
json_store_dict = generate_kerchunk_file_store(netcdf_uri, final_netcdf_uri=final_netcdf_uri, netcdf_product_version='v0.3')

# Run any post processing on the dict
# find-and-replace intermediate file uris, etc
do_stuff(json_store_dict)

# Write the dict as a byte encoded string to a file
fsspec.open('s3://destination_file/for/zarr/store_00_v0.3.zarr', 'wb') as f:
    f.write(json.dumps(json_store_dict).encode())
```

### Combine multiple netcdf4 Zarr Stores

To generate a zarr store for a single stack, use `generate_kerchunk_file_store_stack()`
with a list of the s3 uris for the temporal stack

``` python
from asf_kerchunk_timeseries import generate_kerchunk_file_store_stack

timestep_zarr_stores = ['s3://bucket-name/path/to/netcdf/file_000_v0.3.zarr', ..., 's3://bucket-name/path/to/netcdf/file_400_v0.3.zarr']
timeseries_store_dict = generate_kerchunk_file_store_stack(timestep_zarr_stores)

fsspec.open('s3://destination_file/for/zarr/stack_00.zarr', 'wb') as f:
    f.write(json.dumps(timeseries_store_dict).encode())
```

### aiobotocore session
If credentials are needed to access the s3 bucket for the source netcdf4 data or zarr json stores, an aiobotocore session can be passed to `generate_kerchunk_file_store()`, and two separate aiobotocore sessions can be passed to `generate_kerchunk_file_store_stack()` (for cases where the source netcdf4 data and  zarr json stores are not in the same bucket and the environment doesn't have immediate permission to access both). Kerchunk will use these session(s) to read the s3 file(s).

``` python
# for single timestep
data = generate_kerchunk_file_store(
    netcdf_uri, 
    final_netcdf_uri=final_netcdf_uri, 
    netcdf_product_version='vX.X', 
    session=authenticated_aio_session
    )

# for stack
# if the environment doesn't have default permissions to read from the provided zarr uris,
# OR the netcdf4 data those zarr json stores are referencing,
# separate sessions can be provided for either bucket in a dict via target_opts and remote_opts.
# (kerchunk will fallback to the `Default` profile in aws credentials file, then the current system if that doesn't exist)
stack_data = generate_kerchunk_file_store_stack(
    zarr_json_uris,
    target_opts={'session': session_with_zarr_store_bucket_permissions}
    remote_opts={'session': session_with_netcdf4_bucket_permissions},
)
```

### Advanced
In the consolidation step, if the individual zarr references are in a compressed format (say `gzip`)
`MultiZarrToZarr` may have trouble reading these files because the class expects uncompressed json.
You may need to decompress the gzip files beforehand and then pass the loaded dictionaries directly
to `generate_kerchunk_file_store_stack()` to pass along to `MultiZarrToZarr`.

``` python
# Load uncompressed zarr stores into list of dicts
 mzz_steps = []
 for file in uris:
    with fsspec.open(file, compression='gzip', **fsspec_opts) as f: # fsspec
        mzz_steps.append(json.loads(f.read().decode()))

# pass to wrapper, which will pass references directly to multizarrtozarr
with fsspec.open('test_stack.gz', 'wb', compression='gzip', **output_fsspec_opts) as f:
    consolidated = generate_kerchunk_file_store_stack(mzz_steps)
    f.write(json.dumps(consolidated.translate()).encode())
```
--------