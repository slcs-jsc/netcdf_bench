#!/bin/bash
#SBATCH --time=00:59:00
#SBATCH --account=exaww
#SBATCH --partition=booster
#SBATCH --gres=gpu:1
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=12
#SBATCH --job-name=netcdf_2x2_h0_ind_124GB
#SBATCH --output=./run_netcdf_2x2_h0_ind_124GB_%j.out
#SBATCH --error=./run_netcdf_2x2_h0_ind_124GB_%j.err
set -e

echo "=== NetCDF Benchmark Configuration ==="
echo "Process grid: 2x2 (4 tasks)"
echo "Halo size: 0"
echo "Independent access: yes"
echo "Job started at: $(date)"
echo ""

file_dir=/p/scratch/cslmet/henke1/benchmark/met_input/wind_data_1e8particles_1gpus_12cpus_10x10domains_unevenly_11000x5500x137grid_18dt/

ml purge
ml NVHPC ParaStationMPI netCDF/4.9.2

wind_files=$(find ${file_dir} -name "wind_*.nc" | sort)

echo "Found $(echo $wind_files | wc -w) NetCDF files"
echo "First files: $(echo $wind_files | head -c200)"
echo "Last files: $(echo $wind_files | tail -c200)"

srun ./netcdf_dd_read_bench 0 2 2 1 lon lat $wind_files

echo "Benchmark completed at: $(date)"
