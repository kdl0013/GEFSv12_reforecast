#!/usr/bin/env python3

'''Because all GEFSv12 models are in seperate files for each model.
So save grib2 files as a netcdf, apply a mask to subset to only a specific region.

For this script, it is setup so that you call the variable as an argument when running the 
python script (e.g., python process_GEFS_files.py "tmax_2m")

Source of data:
https://noaa-gefs-retrospective.s3.amazonaws.com/Description_of_reforecast_data.pdf

'''

import os
import datetime as dt
import numpy as np
import xarray as xr
from glob import glob
from multiprocessing import Pool
import sys

# #for Easley cluster
scratch_dir = "" #where all data is currently stored
home_dir = ""
mask_dir = "" #requires mask info that is compatible with CDO operators



'''Compress file size to send back to home computer'''
def cluster_compression_for_GEFS_files(year):
    var=sys.argv[1]
    os.chdir(f'{scratch_dir}/{var}')
    save_dir=f'{scratch_dir}/{var}_processed'
    os.system(f'mkdir {save_dir}')
    
    for file in sorted(glob(f'*{year}*.grib2')):
        file_o = file

        if os.path.exists(xr.open_dataset(f'{save_dir}/{file_o[:-6]}.nc4')):
            pass
        else:

            #Open file
            #Because there are different files, we need to use the proper opening format for each $
            '''Either cf or pf, depending on the file name'''

            var = file_o.split('_')[1] + '_' + file_o.split('_')[2]

            if file_o.split('_')[-1].split('.')[0] == 'c00':
                try:
                    grib_o = xr.open_dataset(file_o,filter_by_keys={'dataType': 'cf'})
                    #save to netcdf to compute daily mean for each variable
                    file_nc_name = f"{file_o[:-6]}.nc4"
                    var_location = f'{scratch_dir}/{var}'
                    grib_o.to_netcdf(f"{var_location}/{file_nc_name}")
                    #Now use cdo operators on the file. Daily mean, remap to CONUS grid, select only CONUS$
                    #remove old nc file (to save on storage space)

                    os.system(f'cdo -b f32 -remapbil,{mask_dir} {var_location}/{file_nc_name} {save_dir}/{file_nc_name}')
                    #os.system(f'cdo -b F32 copy {save_dir}/{file_nc_name} {save_dir}/{file_nc_name}')
                    os.system(f"rm {var_location}/{file_nc_name}")
                except EOFError:
                    pass #error caused by realization having no data
                except ValueError:
                    pass #error caused by realization having no data
            else:
                try:
                    grib_o = xr.open_dataset(file_o,filter_by_keys={'dataType': 'pf'})
                    file_nc_name = f"{file_o[:-6]}.nc4"
                    var_location = f'{scratch_dir}/{var}'
                    grib_o.to_netcdf(f"{var_location}/{file_nc_name}")
                    #Now use cdo operators on the file. Daily mean, remap to CONUS grid, select only CONUS$
                    #remove old nc file (to save on storage space)

                    os.system(f'cdo -b f32 -remapbil,{mask_dir} {var_location}/{file_nc_name} {save_dir}/{file_nc_name}')
                    #os.system(f'cdo -b F32 copy {save_dir}/{file_nc_name} {save_dir}/{file_nc_name}')
                    os.system(f"rm {var_location}/{file_nc_name}")
                except EOFError:
                    pass
                except ValueError:
                    pass #error caused by realization having no data




if __name__ == '__main__':
    year=np.arange(2000,2020)
    p = Pool(6)
    p.map(cluster_compression_for_GEFS_files,year)


