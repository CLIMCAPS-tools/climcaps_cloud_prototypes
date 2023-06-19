"""
Example script to do yearly subset/aggregation on CLIMCAPS data,
including a fully featured variable list for GHG related research.
We primarily focus on CO2 here, including T/CO2 profiles and various
ancillary data fields.
"""

import sys

import os
from datetime import datetime
import climcaps_subaggregation


############
# CLIMCAPS variable list for GHG related research

############
# from main variable group
var_list = ['obs_time_tai93', 'lat', 'lon', 'land_frac', 'surf_alt']

# retrieval vars, each including a _qc and _err
rvars3 = ['air_temp', 'surf_air_temp', 'spec_hum', 'surf_spec_hum', 'h2o_vap_tot',
          'ch4_mmr_midtrop', 'surf_temp', 'cld_frac', 'cld_top_pres', 'surf_ir_emis']
for rv in rvars3:
    var_list += [rv + s for s in ['', '_qc', '_err']]

# other retrieval vars.
var_list += ['air_temp_dof', 'h2o_vap_dof', 'ch4_dof', 'co2_dof',
             'surf_ir_wnum_cnt', 'surf_ir_wnum', 'num_cld', 'num_cld_qc',
             'air_pres', 'air_pres_lay', 'air_pres_lay_bnds']
############
# mw group - possibly as a filter var?
rvars3 = ['mw_air_temp', 'mw_surf_temp']
for rv in rvars3:
    var_list += ['mw/' + rv + s for s in ['', '_qc', '_err']]

############
# mol_lay group - nothing                                                                                                                                                                                       

############
# ave_kern group
var_list += ['ave_kern/' + v for v in [
            'co2_ave_kern', 'co2_func_pres', 'co2_func_last_indx',
            'co2_func_indxs', 'co2_func_htop', 'co2_func_hbot']]

############
# aux group
# more retrievals with _qc and _err
rvars3 = ['co2_vmr', 'for_cld_frac_tot', 'for_cld_top_pres_tot',
          'for_cld_frac_2lay', 'for_cld_top_pres_2lay',]
for rv in rvars3:
    var_list += ['aux/' + rv + s for s in ['', '_qc', '_err']]

var_list += ['aux/' + v for v in [
    'clim_surf_ir_emis', 'clim_surf_ir_wnum', 'clim_surf_ir_wnum_cnt',
    'cldfrc_tot', 'cldfrc_500', 'ampl_eta', 'fg_air_temp', 'fg_surf_temp',
    'fov_weight', 'chi2_temp', 'chi2_h2o', 'chi2_co2', 'pbest', 'pgood',
    'nbest', 'ngood', 'qualtemp', 'qualsurf']]


###########
# platform can be changed to download the data product from other
# satellite platforms.
# change the output_top_dir to the desired download destination.
platform = 'snpp-normal'
short_name = climcaps_subaggregation.get_climcaps_short_name(platform)
output_top_dir = '/home/merrelli/data/CLIMCAPS'

year_string = sys.argv[1]
year = int(year_string)

# create output dirs if needed.
output_dir = os.path.join(output_top_dir, platform)
try:
    os.mkdir(output_dir)
except FileExistsError:
    pass

output_dir = os.path.join(output_top_dir, platform, year_string)
try:
    os.mkdir(output_dir)
except FileExistsError:
    pass

# number of days in the year, and convert to DOY list.
ndays = (datetime(year+1,1,1)-datetime(year,1,1)).days
doy_list = range(1, ndays+1)

##############
# done with setup, now use module code...
climcaps_subaggregation.startup()

t00 = datetime.now()

for doy in doy_list:

    t0 = datetime.now()

    output_file = '{:s}_subaggregate_{:4d}-{:03d}.nc'.format(short_name, year, doy)
    output_fpath = os.path.join(output_dir, output_file)

    if os.access(output_fpath, os.R_OK):
        print('**** skipping yr {:d} doy {:3d}, file already exists'.format(year, doy))
        continue

    print('**** computing yr {:d} doy {:3d}'.format(year, doy))
    climcaps_subaggregation.run_subagg(
        year, doy, platform, var_list, output_fpath, local_download=True)
    print('****     elapsed time (this file): {:s} total: {:s}'.format(
        str(datetime.now()-t0), str(datetime.now()-t00)))
