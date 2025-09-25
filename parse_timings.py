#!/usr/bin/env python3
"""
Parse NetCDF benchmark log files and calculate timing statistics.

This script analyzes log files from NetCDF domain decomposition read benchmarks,
extracting timing data and calculating statistics across nodes and files.
"""

import os
import re
import numpy as np
from pathlib import Path


def parse_log_file(filepath):
    """
    Parse a single log file and extract relevant information.
    
    Returns:
        dict: Dictionary containing parsed data
    """
    data = {
        'filepath': filepath,
        'halo_size': None,
        'process_grid': None,
        'independent_access': None,
        'num_files': None,
        'filesize': None,
        'timings': {},  # rank -> list of times
        'start_time': None
    }
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Extract halo size
    halo_match = re.search(r'Halo size: (\d+)', content)
    if halo_match:
        data['halo_size'] = int(halo_match.group(1))
    
    # Extract process grid
    grid_match = re.search(r'Process grid: (\d+)x(\d+)', content)
    if grid_match:
        data['process_grid'] = (int(grid_match.group(1)), int(grid_match.group(2)))
    
    # Extract independent access
    access_match = re.search(r'Use independent access: (yes|no)', content)
    if access_match:
        data['independent_access'] = access_match.group(1) == 'yes'
    
    # Extract number of files
    files_match = re.search(r'Number of files: (\d+)', content)
    if files_match:
        data['num_files'] = int(files_match.group(1))
    
    # Extract filesize
    filesize_match = re.search(r'filesize=(\d+) bytes', content)
    if filesize_match:
        data['filesize'] = int(filesize_match.group(1))
    
    # Extract timing data for each rank
    timing_pattern = r'rank=(\d+) ; times=([\d\.,]+)'
    timing_matches = re.findall(timing_pattern, content)

    # Extract job start time
    start_time_match = re.search(r'Job started at: (.+)', content)
    if start_time_match:
        data['start_time'] = start_time_match.group(1)

    for rank_str, times_str in timing_matches:
        rank = int(rank_str)
        times = [float(t) for t in times_str.split(',')]
        data['timings'][rank] = times
    
    return data


def calculate_statistics(parsed_data):
    """
    Calculate timing statistics from parsed data.
    
    Args:
        parsed_data: List of dictionaries from parse_log_file
        
    Returns:
        dict: Dictionary containing calculated statistics
    """
    stats = {
        'file_stats': [],  # Per-file statistics
        'overall_stats': {}  # Overall statistics across all files
    }
    
    for data in parsed_data:
        if not data['timings']:
            continue
            
        # Get all ranks and ensure consistent number of files
        ranks = sorted(data['timings'].keys())
        num_files = len(data['timings'][ranks[0]]) if ranks else 0
        
        # Calculate per-file maximum times across all ranks
        max_times_per_file = []
        
        for file_idx in range(num_files):
            # Get timing for this file across all ranks
            file_times = [data['timings'][rank][file_idx] for rank in ranks]
            max_time = max(file_times)
            max_times_per_file.append(max_time)
        
        # Calculate statistics from maximum times
        max_times_array = np.array(max_times_per_file)
        mean_max_time = np.mean(max_times_array)
        std_max_time = np.std(max_times_array)
        
        file_stat = {
            'log_file': os.path.basename(data['filepath']),
            'halo_size': data['halo_size'],
            'process_grid': data['process_grid'],
            'independent_access': data['independent_access'],
            'num_files': data['num_files'],
            'filesize_bytes': data['filesize'],
            'filesize_mb': data['filesize'] / (1024 * 1024) if data['filesize'] else None,
            'num_ranks': len(ranks),
            'max_times_per_file': max_times_per_file,
            'mean_max_time': mean_max_time,
            'std_max_time': std_max_time,
            'min_max_time': np.min(max_times_array),
            'max_max_time': np.max(max_times_array),
            'start_time': data['start_time']
        }
        
        stats['file_stats'].append(file_stat)
    
    return stats


def print_statistics(stats):
    """Print simple statistics list."""
    
    print("NetCDF Benchmark Results:")
    print("Log File                | Mean (s)  | Std (s)   | Size (MB) | Speed (MB/s) | Files | Config")
    print("-" * 105)
    
    for file_stat in stats['file_stats']:
        log_name = file_stat['log_file']
        mean_time = file_stat['mean_max_time']
        std_time = file_stat['std_max_time']
        num_files = file_stat['num_files']
        filesize_mb = file_stat['filesize_mb'] if file_stat['filesize_mb'] else 0
        
        # Calculate I/O speed (MB/s)
        io_speed = filesize_mb / mean_time if mean_time > 0 and filesize_mb > 0 else 0
        
        # Create config string
        grid = f"{file_stat['process_grid'][0]}x{file_stat['process_grid'][1]}" if file_stat['process_grid'] else "N/A"
        halo = file_stat['halo_size'] if file_stat['halo_size'] is not None else "N/A"
        access = "ind" if file_stat['independent_access'] else "col"
        config = f"{grid}, h={halo}, {access}"
        
        print(f"{log_name:<23} | {mean_time:8.6f} | {std_time:8.6f} | {filesize_mb:8.1f} | {io_speed:11.2f} | {num_files:5d} | {config}")
    
    print()

def plot_statistics(stats):
    """Plot statistics using matplotlib."""
    import matplotlib.pyplot as plt
    import datetime

    config_stats = {}

    for file_stat in stats['file_stats']:
        mean_time = file_stat['mean_max_time']
        std_time = file_stat['std_max_time']
        filesize_mb = file_stat['filesize_mb'] if file_stat['filesize_mb'] else 0
        io_speed = filesize_mb / mean_time if mean_time > 0 and filesize_mb > 0 else 0
        io_speed_uncertainty = (std_time / mean_time) * io_speed if mean_time > 0 else 0
        
        grid = f"{file_stat['process_grid'][0]}x{file_stat['process_grid'][1]}" if file_stat['process_grid'] else "N/A"
        halo = file_stat['halo_size'] if file_stat['halo_size'] is not None else "N/A"
        access = "ind" if file_stat['independent_access'] else "col"
        config = f"{grid}, h={halo}, {access}"

        # convert Wed Sep 24 10:19:41 PM CEST 2025 to timestamp
        start_time = datetime.datetime.strptime(file_stat['start_time'], '%a %b %d %I:%M:%S %p %Z %Y') if file_stat['start_time'] else None

        if config not in config_stats:
            config_stats[config] = []
        config_stats[config].append((start_time, mean_time, std_time, io_speed, io_speed_uncertainty))

    # for all filestats sort by start_time
    for config in config_stats:
        config_stats[config].sort(key=lambda x: x[0] if x[0] else datetime.datetime.min)
    
    # plot io speed and uncertainty over time for each config (use an area plot for uncertainty)
    fig, axs = plt.subplots(2, 1, figsize=(12, 12), sharey=True)
    for config, values in config_stats.items():
        times = [v[0] for v in values if v[0] is not None]
        speeds = [v[3] for v in values if v[0] is not None]
        uncertainties = [v[4] for v in values if v[0] is not None]
        mean_speed = np.mean(speeds)
        std_speed = np.std(speeds)
        print(f"{config}: I/O Speed = ({mean_speed:.2f} ± {std_speed:.2f}) MB/s")
        color = "red" if "3x3" in config else "blue"
        linestyle = "-" if "h=2" in config else "--"
        ax = axs[0] if "ind" in config else axs[1]

        if times and speeds:
            ax.plot(times, np.array(speeds), label=config, linestyle=linestyle, marker='x', color=color)
            ax.fill_between(times, np.array(speeds) - np.array(uncertainties), np.array(speeds) + np.array(uncertainties), color=color, alpha=0.2)
    axs[0].set_ylabel('I/O Speed (MB/s)')
    axs[1].set_ylabel('I/O Speed (MB/s)')
    axs[0].set_title('Independent Access')
    axs[1].set_title('Collective Access')
    axs[0].legend()
    axs[1].legend()
    fig.tight_layout()
    fig.savefig("io_speed_over_time.svg")


def main():
    """Main function to process log files and calculate statistics."""
    
    # Define logs directory
    logs_dir = Path(__file__).parent / "logs"
    
    # Find all .out files in logs directory
    log_files = list(logs_dir.glob("*.out"))
    
    if not log_files:
        print(f"No log files found in {logs_dir}")
        return
    
    print(f"Found {len(log_files)} log file(s) to process:")
    for log_file in log_files:
        print(f"  - {log_file.name}")
    print()
    
    # Parse all log files
    parsed_data = []
    for log_file in log_files:
        try:
            data = parse_log_file(log_file)
            parsed_data.append(data)
            print(f"Successfully parsed: {log_file.name}")
        except Exception as e:
            print(f"Error parsing {log_file.name}: {e}")
    
    if not parsed_data:
        print("No data could be parsed from log files.")
        return
    
    print()
    
    # Calculate statistics
    stats = calculate_statistics(parsed_data)
    
    print_statistics(stats)

    plot_statistics(stats)


if __name__ == "__main__":
    main()
