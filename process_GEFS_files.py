#!/usr/bin/env python3

'''Because all GEFSv12 models are in seperate files for each model.
So save grib2 files as a netcdf, apply a mask to subset to only a specific region.

Also I have it set to convert all data to a float32 called using CDO operators.

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
data_dir = "" #where all data is currently stored
mask_file = "" #requires mask info that is compatible with CDO operators

#Make sure to pass the var name in when calling the python script.

def subset_by_mask_for_GEFS_files(year):
    var=sys.argv[1]
    os.chdir(f'{data_dir}/{var}')
    
    save_dir=f'{data_dir}/{var}_processed'
    os.system(f'mkdir {save_dir}')
    
    for file in sorted(glob(f'*{year}*.grib2')):
        file_o = file #Just keep this here

        if os.path.exists(xr.open_dataset(f'{save_dir}/{file_o[:-6]}.nc4')):
            pass
        else:
            '''Either cf or pf, depending on the file name'''
            var = file_o.split('_')[1] + '_' + file_o.split('_')[2]
            var_location = f'{data_dir}/{var}'
            
            if file_o.split('_')[-1].split('.')[0] == 'c00':
                try:
                    grib_o = xr.open_dataset(file_o,filter_by_keys={'dataType': 'cf'})
                    file_nc_name = f"{file_o[:-6]}.nc4"
                    grib_o.to_netcdf(f"{var_location}/{file_nc_name}")

                    #Remap to specific grid
                    os.system(f'cdo -b f32 -remapbil,{mask_file} {var_location}/{file_nc_name} {save_dir}/{file_nc_name}')
                    os.system(f"rm {var_location}/{file_nc_name}")
                except EOFError:
                    pass #error caused by realization having no data
                except ValueError:
                    pass #error caused by realization having no data
            else:
                try:
                    grib_o = xr.open_dataset(file_o,filter_by_keys={'dataType': 'pf'})
                    file_nc_name = f"{file_o[:-6]}.nc4"
                    grib_o.to_netcdf(f"{var_location}/{file_nc_name}")

                    os.system(f'cdo -b f32 -remapbil,{mask_file} {var_location}/{file_nc_name} {save_dir}/{file_nc_name}')
                    os.system(f"rm {var_location}/{file_nc_name}")
                except EOFError:
                    pass
                except ValueError:
                    pass




if __name__ == '__main__':
    year=np.arange(2000,2020)
    p = Pool(6)
    p.map(subset_by_mask_for_GEFS_files,year)


