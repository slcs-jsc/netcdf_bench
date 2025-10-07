#!/bin/bash
#SBATCH --time=00:59:00
#SBATCH --account=exaww
#SBATCH --partition=booster
#SBATCH --gres=gpu:1
#SBATCH --nodes=100
#SBATCH --ntasks=100
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=12
#SBATCH --job-name=netcdf_10x10_h0_ind
#SBATCH --output=./run_netcdf_10x10_h0_ind_%j.out
#SBATCH --error=./run_netcdf_10x10_h0_ind_%j.err
set -e

echo "=== NetCDF Benchmark Configuration ==="
echo "Process grid: 10x10 (100 tasks)"
echo "Halo size: 0"
echo "Independent access: yes"
echo "Job started at: $(date)"
echo ""

file_dir=/p/scratch/cslmet/henke1/benchmark/met_input/wind_data_1e8particles_1gpus_12cpus_10x10domains_unevenly_11000x5500x137grid_18dt/

ml purge
ml NVHPC ParaStationMPI netCDF/4.9.2

wind_files=$(find ${file_dir} -name "wind_*.nc" | sort | head -n 10)

echo "Found $(echo $wind_files | wc -w) NetCDF files"
echo "First files: $(echo $wind_files | head -c200)"
echo "Last files: $(echo $wind_files | tail -c200)"

srun ./netcdf_dd_read_bench 0 10 10 1 lon lat $wind_files

echo "Benchmark completed at: $(date)"
