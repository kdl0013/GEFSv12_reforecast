#!/bin/bash
module load conda
conda info --envs
conda activate /glade/work/klesinger/conda-envs/tf212gpu
module load ncl
module load cdo

#export NCARG_ROOT=/usr/local

NH_grid=/glade/work/klesinger/FD_RZSM_deep_learning/Data/masks/Northern_Hemisphere_0.5grid.grd

#First create wget scripts 

#Argv 1 is the script directory where files are to be saved
script=wget_scripts
#Argv 2 is the directory where raw files will be downloaded
save=/glade/scratch/klesinger/GEFSv12_raw

python3 create_wget_scripts.py $script $save

for file in wget_scripts;do
bash $file;
done

process_hgt_pressure () {

#Now process hgt_pres individually
process_dir_hgt=$save/hgt_pres/hgt_pres_processed
mkdir $process_dir_hgt

cd $save/hgt_pres

pressure_level=$1

for file in *.grib2; do
    file_name="${file%??????}" #Must remove the last 6 characters in string
    final_out=$process_dir_hgt/"${file_name}".nc
    if test -f $final_out; then
        echo "File already exists"
    else
        ncl_convert2nc $file -L
        #Now use cdo to convert to specific coordinates
        cdo remapbil,$NH_grid -sellevel,"${pressure_level}" "${file_name}".nc $final_out
    fi
done
}

process_hgt_pressure "20000" #Inset the pressure level desired


process_other_variables () {

#Now process hgt_pres individually
process_dir=$save/$1/"${1}"_processed
mkdir $process_dir

cd $save/$1

pressure_level=$1

for file in *.grib2; do
    file_name="${file%??????}" #Must remove the last 6 characters in string
    final_out=$process_dir/"${file_name}".nc
    if test -f $final_out; then
        echo "File already exists"
    else
        ncl_convert2nc $file -L
        #Now use cdo to convert to specific coordinates
        cdo remapbil,$NH_grid "${file_name}".nc $final_out
    fi
done
}

process_other_variables "soilw_bgrnd"
