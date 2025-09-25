#!/bin/bash

ml purge
ml NVHPC ParaStationMPI netCDF

mpicc netcdf_dd_read_bench.c -o netcdf_dd_read_bench -lnetcdf