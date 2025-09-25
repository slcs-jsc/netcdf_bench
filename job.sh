#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --account=exaww
#SBATCH --partition=booster
#SBATCH --gres=gpu:1
#SBATCH --nodes=9
#SBATCH --ntasks=9
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=12
#SBATCH --output=./logs/run_%j.out
#SBATCH --error=./logs/run_%j.err
set -e

file_dir=/p/scratch/cslmet/henke1/benchmark/met_input/wind_data_1e7particles_1gpus_12cpus_3x3domains_unevenly_1200x600x120grid_180dt/

ml purge
ml NVHPC ParaStationMPI netCDF/4.9.2

wind_files=$(find ${file_dir} -name "wind_*.nc" | sort)

echo "Found $(echo $wind_files | wc -w) NetCDF files"
echo "First files: $(echo $wind_files | head -c200)"
echo "Last files: $(echo $wind_files | tail -c200)"

srun ./netcdf_dd_read_bench 0 3 3 1 lon lat $wind_files

echo "Benchmark completed at: $(date)"
