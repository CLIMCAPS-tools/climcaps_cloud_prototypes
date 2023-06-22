import os
from datetime import datetime, timedelta
from collections import OrderedDict
from copy import copy

import numpy as np
import netCDF4

import earthaccess

def startup():
    try:
        earthaccess.login(strategy='netrc')
    except FileNotFoundError:
        earthaccess.login(strategy="interactive", persist=True)

def get_climcaps_short_name(platform):
    climcaps_short_names = {
        'snpp-normal'  : "SNDRSNIML2CCPRETN",
        'snpp-full'    : "SNDRSNIML2CCPRET",
        'jpss1'        : "SNDRJ1IML2CCPRET"
    }
    return climcaps_short_names[platform]

def get_granule_list(year, doy, short_name):
    """ use earthaccess.search_data to get a list of granules, based
    on a year and Day Of Year (DOY).
    Trims the first and last file, if necessary.
    """

    year_start = datetime(year, 1, 1)
    window_start = year_start + timedelta(days = doy-1)
    window_stop = window_start + timedelta(days = 1)

    granule_list = earthaccess.search_data(
        short_name = short_name, cloud_hosted=True,
        temporal=(window_start.strftime('%Y-%m-%d'), window_stop.strftime('%Y-%m-%d')),
    )

    # the way the time range is applied (I think it is 'less than or equal to'),
    # we get the last granule from the previous day, and the first granule from
    # the next day. Since the temporal range is only Y-M-D we have to manually
    # trim the extra granules at the end. We want just the one day
    # (granules 1 - 240). So, check the first and last granule to make sure they are
    # from the requested day; if not, remove from list.
    # example filename:
    # SNDR.J1.CRIMSS.20191009T2354.m06.g240.L2_CLIMCAPS_RET.std.v02_28.G.200215102526.nc

    # granule list could be zero (if there is a data gap) - in which case granule_list
    # could be an empty list.
    if len(granule_list) > 0:
        expected_ymd = window_start.strftime('%Y%m%d')
        for idx in (0, -1):
            filename = granule_list[idx].data_links()[idx].split('/')[-1]
            ymd = filename.split('.')[3][:8]
            if ymd != expected_ymd:
                _ = granule_list.pop(idx)

    return granule_list

def _load_from_netCDF4(nc, var_list):
    """ helper function to load variables from an already created netCDF object.
    callers will create the netCDF object depending on whether it is an s3
    file-like object or an actual file; this loading loop is identical otherwise,
    so it is split into a helper function."""
    dat = {}
    dims = {}
    attrs = {}
    for v in var_list:
        dat[v] = nc[v][:]
        dims[v] = {'names':list(), 'sizes':list()}
        # apparently, dimensions can stored at the group level, and there
        # doesn't seem to be an easy way to determine where the dimension is
        # defined. So, use a try/catch ? I don't have a better idea.
        # this will have the side effect of moving the dimension from the group
        # to the main file, but I think that will be a small issue.
        for dim in nc[v].dimensions:
            dims[v]['names'].append(dim)
            try:
                dims[v]['sizes'].append(nc.dimensions[dim].size)
            except KeyError:
                grp = v.split('/')[0]
                dims[v]['sizes'].append(nc[grp].dimensions[dim].size)

        attrs[v] = {a:nc[v].getncattr(a) for a in nc[v].ncattrs()}

    return dat, dims, attrs

        
def load_granule_from_file(fname, var_list):
    """ read the desired variables into a python dictionary, starting from a s3 object.
    outputs are the same as load_granule_from_s3."""

    with netCDF4.Dataset(fname, 'r') as nc:
        dat, dims, attrs = _load_from_netCDF4(nc, var_list)

    return dat, dims, attrs


def load_granule_from_s3(s3_obj, var_list):
    """ read the desired variables into a python dictionary, starting from a s3 object.

    returns dat (the data arrays), dims (dimensions), and attrs (netcdf attributes),
    all python dictionaries keyed by the var_list.
    
    dat: values are the ndarrays loaded from the netCDF4 file
    dims: values are a two key dictionary, 'names' (the dimension names) and
        'sizes' (the dimension sizes). For the names and sizes, the order matters
        (hence storing them in a list) and it is also useful to be able to check if
        a certain dim name is present (hence storing the names in their own list.)
    attrs: values are another python dictionary, containing the name/value pairs for
        each attribute contained in the file.
    """

    nc_bytes = s3_obj.read()
    if len(nc_bytes) == 0:
        raise OSError('s3 read failed to return any data')

    with netCDF4.Dataset('inmemory.nc', memory=nc_bytes, mode = 'r') as nc:
        dat, dims, attrs = _load_from_netCDF4(nc, var_list)
            
    return dat, dims, attrs


def concat_granules(dat_list, dims):
    """
    concatenates the list of data arrays (dat_list) into a single
    python dictionary. concatenation is done on the leading (atrack) axis
    if the variable has that dimension. Also requires a dims dictionary (as
    returned from the load_granule_from_s3() function.
    If not, then the concatenate list will have a copy of what was contained
    in the first element of dat_list, basically assuming this is static data
    that is not changing along track.
    
    returns:
    cdat: concatenated data, with the same keys as were present in each dat_list
         entry (e.g., the original netcdf variable names)
    cdims: the concatenated dimensions. Should be the same as the input dims, but
         any atrack dimension sizes are updated with the size in the contcatenated
         data arrays.
    """
    cdat = {}
    for v in dat_list[0]:
        cdat[v] = dat_list[0][v].copy()

    for dat in dat_list[1:]:
        for v in dat:
            if dims[v]['names'] == []:
                continue
            # note we are assuming atrack is the leading dimension (axis=0)
            if 'atrack' in dims[v]['names']:
                cdat[v] = np.concatenate([cdat[v], dat[v]], axis=0)

    # copy dims, and update atrack length
    # assumes that atrack is the leading dimension. we just need to loop over
    # all variables to find the first one that has atrack dimension.
    cdims = {}
    for v in cdat:
        new_dim_names = copy(dims[v]['names'])
        new_dim_sizes = copy(dims[v]['sizes'])
        if len(new_dim_names) > 0:
            if new_dim_names[0] == 'atrack':
                new_dim_sizes[0] = cdat[v].shape[0]
        cdims[v] = {'names':new_dim_names, 'sizes':new_dim_sizes}

    return cdat, cdims


def write_cdat(cdat, dims, attrs, fname):
    """
    write the concatenated data to a new netCDF4 file.
    dims, attrs must be passed forward from one return from the load function.
    
    fname is the desired output filename.
    """

    # merge all used dims into a single list
    merged_dim_list = OrderedDict()
    for v in dims:
        for dim, dim_size in zip(dims[v]['names'], dims[v]['sizes']):
            merged_dim_list[dim] = dim_size

    with netCDF4.Dataset(fname, 'w') as nc:
        for dim, dim_size in merged_dim_list.items():
            nc.createDimension(dim, dim_size)
        for v in cdat:
            if '_FillValue' in attrs[v]:
                fill_value = attrs[v]['_FillValue']
            else:
                fill_value = None
            # string needs to be special-cased: the numpy array we get from
            # the netCDF4 library has dtype object, so we can't use the
            # dtype as the data type for the created NC variable.
            if cdat[v].dtype == object:
                # check the first element to make sure it is a python str.
                # if not, this is a case we do not handle, so throw exception.
                # the syntax here is supposed to be a simple way to check the
                # first element without knowing the ndim or shape.
                if type(cdat[v].flat[0]) == str:
                    output_dtype = str
                else:
                    raise ValueError(
                        'concatenated array has type object, and does not '+
                        'contain python strings. Cannot write batch to netCDF4')
            else:
                output_dtype = cdat[v].dtype

            ncv = nc.createVariable(
                v, output_dtype,
                dimensions = tuple(dims[v]['names']),
                compression = 'zlib',
                complevel = 1,
                fill_value = fill_value)
            ncv[:] = cdat[v]
            # TBD: do we need to special case for setncattr_string?
            for aname, avalue in attrs[v].items():
                if aname == '_FillValue':
                    continue
                ncv.setncattr(aname, avalue)

def _earthaccess_download(granule_list, tmp_dir):

    try:
        os.mkdir(tmp_dir)
    except FileExistsError:
        pass

    # earthaccess.download() can often fail on random files in the granule list.
    # this means there are 'Exception' objects in the list.
    # as a hacky solution, if there are any non-strings in the list, simply re-run
    # the download call again, up to three times.
    max_num_tries = 3
    num_tries = 0
    num_expected_files = len(granule_list)
    num_downloaded_files = 0
    while (num_tries < max_num_tries) and (num_downloaded_files < num_expected_files):
        files = earthaccess.download(granule_list, tmp_dir)
        check_list = list(map(type, files))
        num_downloaded_files = check_list.count(str)

    # need to add the local dir, earthaccess only returns the filenames
    downloaded_filepaths = [os.path.join(tmp_dir, f) for f in files]

    return downloaded_filepaths


def run_subagg(year, doy, platform, var_list, output_file, local_download=False,
               tmp_dir='climcaps_subagg_tmp'):
    """
    run all the pieces, to create one daily subaggregate file.
    """

    short_name = get_climcaps_short_name(platform)
    granule_list = get_granule_list(year, doy, short_name)

    if len(granule_list) == 0:
        print('Year {:d}, DOY {:d} returned no granules'.format(year, doy))
        return

    dat_list = []

    if local_download:
        downloaded_filepaths = _earthaccess_download(granule_list, tmp_dir)
        for fpath in downloaded_filepaths:
            dat, dims, attrs = load_granule_from_file(fpath, var_list)
            dat_list.append(dat)
            os.remove(fpath)
    else:
        s3_objs = earthaccess.open(granule_list)
        for s3_obj in s3_objs:
            dat, dims, attrs = load_granule_from_s3(s3_obj, var_list)
            dat_list.append(dat)
            # not sure we need to do this, but there is a close method.
            s3_obj.close()
        
    cdat, cdims = concat_granules(dat_list, dims)
    
    write_cdat(cdat, cdims, attrs, output_file)
