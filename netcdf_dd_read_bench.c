// Include necessary libraries for MPI, netCDF, and other utilities
#include <mpi.h>
#include <netcdf.h>
#include <netcdf_par.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

// Function to get the current time in seconds for performance measurement
double get_time_sec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + 1e-6 * tv.tv_usec;
}

// Function to safely abort MPI processes in case of errors
void safe_abort(MPI_Comm comm, int errorcode) {
    fflush(stdout);
    fflush(stderr);
    usleep(100000); // 100ms delay to flush output buffers
    MPI_Abort(comm, errorcode);
}

int main(int argc, char **argv) {
    // Initialize MPI and get rank and size
    int rank, nprocs;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    // Check for correct number of arguments
    if (argc < 8) {
        if (rank == 0)
            printf("Usage: %s <halo> <nproc_x> <nproc_y> <use_independent> <xdim_name> <ydim_name> <file1.nc> [file2.nc ...]\n", argv[0]);
        MPI_Finalize();
        return 1;
    }

    // Parse command-line arguments
    int halo = atoi(argv[1]);
    int nproc_x = atoi(argv[2]);
    int nproc_y = atoi(argv[3]);
    int use_independent = atoi(argv[4]);
    char *lon_name = argv[5];
    char *lat_name = argv[6];
    int nfiles = argc - 7;
    char **file_list = &argv[7];

    // Ensure the number of processes matches the decomposition grid
    if (nprocs != nproc_x * nproc_y) {
        if (rank == 0)
            printf("Error: nprocs != nproc_x * nproc_y\n");
        MPI_Finalize();
        return 1;
    }

    // For 1x1 domains, force halo to be 0 to avoid problems
    if (nproc_x == 1 && nproc_y == 1 && halo > 0) {
        if (rank == 0)
            printf("Warning: 1x1 domain decomposition detected, forcing halo=0\n");
        halo = 0;
    }

    // Print configuration details from rank 0
    if (rank == 0) {
        printf("Halo size: %d\n", halo);
        printf("Process grid: %dx%d\n", nproc_x, nproc_y);
        printf("Use independent access: %s\n", use_independent ? "yes" : "no");
        printf("Number of files: %d\n", nfiles);
    }

    // Calculate process coordinates in the grid
    int px = rank % nproc_x;
    int py = rank / nproc_x;

    // Open the first netCDF file in parallel mode
    int ncid;
    int retval;
    retval = nc_open_par(file_list[0], NC_NOWRITE, MPI_COMM_WORLD, MPI_INFO_NULL, &ncid);
    if (retval != NC_NOERR) { 
        printf("Rank %d: Error opening file %s: %s\n", rank, file_list[0], nc_strerror(retval)); 
        safe_abort(MPI_COMM_WORLD, 1); 
    }

    // Query the number of dimensions and variables in the file
    int nvars, ndims;
    retval = nc_inq(ncid, &ndims, &nvars, NULL, NULL);
    if (retval != NC_NOERR) { 
        printf("Error querying number of dimensions & variables in file %s\n", file_list[0]); 
        safe_abort(MPI_COMM_WORLD, 1); 
    }
    int lat_idx = -1;
    int lon_idx = -1;

    int dimvars = 0;
    size_t *dimlen = (size_t *)malloc(ndims * sizeof(size_t));
    int *is_dimvar = (int *)calloc(nvars, sizeof(int));
    for (int varid = 0; varid < nvars; varid++) {
        is_dimvar[varid] = 0;
    }
    for (int dimid = 0; dimid < ndims; dimid++) {
        char dim_name[NC_MAX_NAME + 1];
        retval = nc_inq_dim(ncid, dimid, dim_name, &dimlen[dimid]);
        if (retval != NC_NOERR) {
            printf("Error querying dimension ID %d in file %s\n", dimid, file_list[0]);
            safe_abort(MPI_COMM_WORLD, 1);
        }
        for (int varid = 0; varid < nvars; varid++) {
            char var_name[NC_MAX_NAME + 1];
            retval = nc_inq_varname(ncid, varid, var_name);
            if (retval != NC_NOERR) {
                printf("Error querying variable name for varid %d in file %s\n", varid, file_list[0]);
                safe_abort(MPI_COMM_WORLD, 1);
            }
            if (strcmp(var_name, dim_name) == 0) {
                dimvars++;
                is_dimvar[varid] = 1;
                if (strcmp(lon_name, dim_name) == 0) {
                    lon_idx = dimid;
                    if (rank == 0)
                        printf("Found lon dimension at index %d\n", lon_idx);
                }else if (strcmp(lat_name, dim_name) == 0) {
                    lat_idx = dimid;
                    if (rank == 0)
                        printf("Found lat dimension at index %d\n", lat_idx);
                }
                break;
            }
        }
        if (rank == 0) {
            printf("  Dimension %d: name='%s', length=%zu\n", dimid, dim_name, dimlen[dimid]);
        }
    }
    nc_close(ncid);
    if (lat_idx == -1 || lon_idx == -1) {
        printf("Error: Could not find %s/%s dimensions in file %s\n", lat_name, lon_name, file_list[0]);
        safe_abort(MPI_COMM_WORLD, 1);
    }
    nvars -= dimvars;
    if (rank == 0) {
        printf("First file contains %d dimensions and %d variables (+ %d dimension variables)\n", ndims, nvars, dimvars);
    }

    // Calculate subdomain boundaries for each process
    int lon_size = dimlen[lon_idx], lat_size = dimlen[lat_idx];
    int sub_lon = lon_size / nproc_x;
    int sub_lat = lat_size / nproc_y;
    int base_lon0 = px * sub_lon - halo;
    int base_lat0 = py * sub_lat;
    int base_lon1 = px * sub_lon + sub_lon - 1 + halo;
    int base_lat1 = py * sub_lat + sub_lat - 1;

    // check for periodic boundaries
    int has_periodic_halo = (halo > 0) && ( (px == 0) || (px == nproc_x - 1) );
    int periodic_halo_lon_start = 0;
    if (px == 0) {
        base_lon0 += halo;
        periodic_halo_lon_start = lon_size - halo - 1;
    }else if (px == nproc_x - 1) {
        base_lon1 -= halo;
        periodic_halo_lon_start = 0;
    }

    // Allocate buffer for reading data including halos
    size_t bufsize = (sub_lat + 2*halo) * (sub_lon + 2*halo);
    for (int d = 0; d < ndims; d++) {
        if (d != lat_idx && d != lon_idx) {
            bufsize *= dimlen[d];
        }
    }
    float *buffer = (float*) malloc(bufsize * sizeof(float));

    if (rank == 0) {
        printf("Processing %d files with %d ranks (%dx%d decomposition, halo=%d)\n", nfiles, nprocs, nproc_x, nproc_y, halo);
    }
    printf("Rank %d: subdomain lat[%d:%d], lon[%d:%d]%s\n", rank, base_lat0, base_lat1, base_lon0, base_lon1, has_periodic_halo ? " with periodic halo" : "");

    // Calculate the size of the file in bytes for timing output
    size_t file_bytes = sizeof(float) * nvars;
    for (int i = 0; i < ndims; i++) {
        file_bytes *= dimlen[i];
    }
    
    double *file_times = (double*) malloc(nfiles * sizeof(double));
    size_t *start = (size_t*) malloc(ndims * sizeof(size_t));
    size_t *count = (size_t*) malloc(ndims * sizeof(size_t));
    for (int d = 0; d < ndims; d++) {
        start[d] = 0;
        count[d] = dimlen[d];
    }

    for (int f = 0; f < nfiles; f++) {
        // Open each netCDF file in parallel mode
        retval = nc_open_par(file_list[f], NC_NOWRITE, MPI_COMM_WORLD, MPI_INFO_NULL, &ncid);
        if (retval != NC_NOERR) { 
            printf("Rank %d: Error opening file %s: %s\n", rank, file_list[f], nc_strerror(retval)); 
            safe_abort(MPI_COMM_WORLD, 1); 
        }

        // Set parallel access mode for all data variables (skip dimension variables)
        for (int varid = 0; varid < nvars+dimvars; varid++) {
            if (is_dimvar[varid]) continue;
            retval = nc_var_par_access(ncid, varid, use_independent ? NC_INDEPENDENT : NC_COLLECTIVE);
            if (retval != NC_NOERR) { 
                printf("Rank %d: Error setting %s access for var %d: %s\n", 
                       rank, use_independent ? "independent" : "collective", varid, nc_strerror(retval)); 
                safe_abort(MPI_COMM_WORLD, 1);
            }
        }

        double file_start = get_time_sec();
        for (int varid = 0; varid < nvars+dimvars; varid++) {
            if (is_dimvar[varid]) continue;
            // Read the subdomain for this variable
            start[lat_idx] = base_lat0;
            start[lon_idx] = base_lon0;
            count[lat_idx] = base_lat1-base_lat0+1;
            count[lon_idx] = base_lon1-base_lon0+1;
            retval = nc_get_vara_float(ncid, varid, start, count, buffer);
            if (retval != NC_NOERR) {
                printf("Rank %d: Error reading subdomain for var %d: %s\n", rank, varid, nc_strerror(retval));
                safe_abort(MPI_COMM_WORLD, 1);
            }
            buffer[0] *= 3.4;
            // Read periodic halo if applicable
            if (halo > 0 && has_periodic_halo) {
                start[lon_idx] = periodic_halo_lon_start;
                count[lon_idx] = halo;
                retval = nc_get_vara_float(ncid, varid, start, count, buffer);
                if (retval != NC_NOERR) {
                    printf("Rank %d: Error reading periodic halo for var %d: %s\n", rank, varid, nc_strerror(retval));
                    safe_abort(MPI_COMM_WORLD, 1);
                }
                buffer[0] *= 3.4;
            }
        }
        nc_close(ncid);
        MPI_Barrier(MPI_COMM_WORLD);
        double file_end = get_time_sec();
        file_times[f] = file_end - file_start;
    }

    // Gather timing results from all ranks
    double *all_times = NULL;
    if (rank == 0) {
        all_times = (double*) malloc(nprocs * nfiles * sizeof(double));
    }
    
    // Gather all file times to rank 0
    MPI_Gather(file_times, nfiles, MPI_DOUBLE, all_times, nfiles, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    
    // Print results from rank 0
    if (rank == 0) {
        printf("filesize=%f MB\n", (float)(file_bytes)/1e6);
        for (int r = 0; r < nprocs; r++) {
            printf("rank=%d ; times=", r);
            for (int f = 0; f < nfiles; f++) {
                printf("%.6f", all_times[r * nfiles + f]);
                if (f < nfiles - 1) printf(",");
            }
            printf("\n");
        }
        free(all_times);
    }

    free(start);
    free(count);
    free(dimlen);
    free(is_dimvar);
    free(file_times);
    MPI_Finalize();
    return 0;
}