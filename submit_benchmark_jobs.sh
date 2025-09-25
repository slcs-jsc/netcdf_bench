#!/bin/bash

# Script to generate and submit multiple NetCDF benchmark jobs with different configurations
# Configurations: 3x3/1x1 grid, h=2/h=0 halo, ind=yes/no access
# Total: 8 different configurations

set -e

# Configuration arrays
grids=("3 3" "1 1")
grid_names=("3x3" "1x1")
halos=(2 0)
independent=(1 0)
independent_names=("ind" "col")

# Number of hours to spread jobs across (default: 8, can be overridden as first argument)
N_HOURS=${1:-8}

echo "Submitting NetCDF benchmark jobs with staggered scheduling over $N_HOURS hours"
echo "Total jobs to submit: $((8 * N_HOURS))"
echo ""

# Base directories for files (different for each grid configuration)
file_dir_3x3="/p/scratch/cslmet/henke1/benchmark/met_input/wind_data_1e7particles_1gpus_12cpus_3x3domains_unevenly_2400x1200x120grid_90dt/"
file_dir_1x1="/p/scratch/cslmet/henke1/benchmark/met_input/wind_data_1e7particles_1gpus_12cpus_1x1domains_unevenly_2400x1200x120grid_90dt/"

# Create logs and scripts directories if they don't exist
mkdir -p logs
mkdir -p scripts

job_counter=0

echo "=== Creating 8 unique job scripts ==="

# First, create the 8 unique job scripts (one for each configuration)
for grid_idx in 0 1; do
    grid="${grids[$grid_idx]}"
    grid_name="${grid_names[$grid_idx]}"
    
    # Calculate nodes and tasks based on grid dimensions
    grid_x=$(echo $grid | cut -d' ' -f1)
    grid_y=$(echo $grid | cut -d' ' -f2)
    num_tasks=$((grid_x * grid_y))
    
    # Select appropriate file directory
    if [ "$grid_name" = "3x3" ]; then
        file_dir="$file_dir_3x3"
    else
        file_dir="$file_dir_1x1"
    fi
    
    for halo in "${halos[@]}"; do
        for ind_idx in 0 1; do
            ind="${independent[$ind_idx]}"
            ind_name="${independent_names[$ind_idx]}"
            
            job_counter=$((job_counter + 1))
            
            # Create base job name (without time slot)
            job_name="netcdf_${grid_name}_h${halo}_${ind_name}"
            job_file="scripts/job_${job_name}.sh"
            
            echo "Creating job script $job_counter/8: $job_name"
                
            # Generate the job script
            cat > "$job_file" << EOF
#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --account=exaww
#SBATCH --partition=booster
#SBATCH --gres=gpu:1
#SBATCH --nodes=$num_tasks
#SBATCH --ntasks=$num_tasks
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=12
#SBATCH --job-name=$job_name
#SBATCH --output=./logs/run_${job_name}_%j.out
#SBATCH --error=./logs/run_${job_name}_%j.err
set -e

echo "=== NetCDF Benchmark Configuration ==="
echo "Process grid: $grid_name ($num_tasks tasks)"
echo "Halo size: $halo"
echo "Independent access: $([ $ind -eq 1 ] && echo "yes" || echo "no")"
echo "Job started at: \$(date)"
echo ""

file_dir=$file_dir

ml purge
ml NVHPC ParaStationMPI netCDF/4.9.2

wind_files=\$(find \${file_dir} -name "wind_*.nc" | sort)

echo "Found \$(echo \$wind_files | wc -w) NetCDF files"
echo "First files: \$(echo \$wind_files | head -c200)"
echo "Last files: \$(echo \$wind_files | tail -c200)"

srun ./netcdf_dd_read_bench $halo $grid $ind lon lat \$wind_files

echo "Benchmark completed at: \$(date)"
EOF
            
            echo "  → Job script created: $job_file"
            
        done
    done
done

echo ""
echo "=== Submitting jobs with staggered scheduling ==="

# Now submit each job script multiple times with different begin times
job_scripts=(scripts/job_netcdf_*.sh)
submission_counter=0

for hour in $(seq 0 $((N_HOURS - 1))); do
    echo "=== Hour $hour ==="
    
    # Calculate delay
    if [ $hour -eq 0 ]; then
        delay_option=""
        delay_str="now"
    else
        delay_option="--begin=now+${hour}hour"
        delay_str="now+${hour}h"
    fi
    
    for job_file in "${job_scripts[@]}"; do
        submission_counter=$((submission_counter + 1))
        job_name=$(basename "$job_file" .sh | sed 's/job_//')
        
        echo "Submitting job $submission_counter/$((8 * N_HOURS)): $job_name (scheduled for $delay_str)"
        
        # Submit the job
        if [ -n "$delay_option" ]; then
            sbatch_output=$(sbatch $delay_option "$job_file")
        else
            sbatch_output=$(sbatch "$job_file")
        fi
        
        # Extract job ID from sbatch output
        job_id=$(echo "$sbatch_output" | grep -o '[0-9]\+')
        echo "  → Submitted as job ID: $job_id"
        
    done
    echo ""
done

echo "All jobs submitted successfully!"
echo ""
echo "Configuration summary:"
echo "- Process grids: 3x3, 1x1"
echo "- Halo sizes: 2, 0"
echo "- Access types: independent, collective"
echo "- Time slots: $N_HOURS hours"
echo "- Total jobs: $((8 * N_HOURS))"
echo ""
echo "Monitor job status with: squeue -u \$USER"
echo "Check logs in: ./logs/"
