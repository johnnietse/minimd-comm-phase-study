/* ----------------------------------------------------------------------
   miniMD is a simple, parallel molecular dynamics (MD) code.
   ... (License Header) ...
---------------------------------------------------------------------- */

//#define PRINTDEBUG(a) a
#define PRINTDEBUG(a)
#include "stdio.h"
#include "integrate.h"
#include "openmp.h"
#include "math.h"
#include "atom.h"
#include "force.h"
#include "neighbor.h"
#include "comm.h"
#include "thermo.h"
#include "timer.h"
#include <mpi.h>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cerrno>
#include <vector>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdlib.h>

// ========================================================================
// SECTION 1: INSTRUMENTATION / FLAGGING SYSTEM
// ========================================================================

// Tuning: How many records to keep in RAM before dumping to disk?
#define LOG_BUFFER_SIZE 1000 

// Phase Enums for efficiency
enum PhaseType {
    PHASE_SERIAL_COMPUTE,
    PHASE_PARALLEL_COMPUTE,
    PHASE_COMMUNICATION,
    PHASE_MEMORY_BOUND,
    PHASE_IO,
    PHASE_FINISHED
};

struct LogEntry {
    double timestamp;
    PhaseType phase;
};

static LogEntry log_buffer[LOG_BUFFER_SIZE];
static int buffer_count = 0;
static PhaseType last_logged_phase = PHASE_FINISHED; // Init state
static bool hint_mode_enabled = false;
static bool hint_checked = false;

double get_epoch_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

const char* get_phase_string(PhaseType p) {
    switch(p) {
        case PHASE_SERIAL_COMPUTE: return "SERIAL_COMPUTE";
        case PHASE_PARALLEL_COMPUTE: return "PARALLEL_COMPUTE";
        case PHASE_COMMUNICATION: return "COMMUNICATION";
        case PHASE_MEMORY_BOUND: return "MEMORY_BOUND";
        case PHASE_IO: return "IO_STORAGE";
        default: return "UNKNOWN";
    }
}

// Flushes the RAM buffer to the Disk CSV
void flush_buffer_to_disk() {
    FILE* f = fopen("ground_truth.csv", "a"); // Append mode
    if (!f) return;
    
    for (int i = 0; i < buffer_count; i++) {
        fprintf(f, "%.6f,%s\n", log_buffer[i].timestamp, get_phase_string(log_buffer[i].phase));
    }
    buffer_count = 0; // Reset buffer
    fclose(f);
}

// Main Instrumentation Function
void update_phase(PhaseType new_phase, int my_rank) {
    // Only Rank 0 records Ground Truth (Serial Condition)
    if (my_rank != 0) return;

    // 1. Env Var Check (Once)
    if (!hint_checked) {
        const char* env = getenv("MINIMD_HINT_MODE");
        if (env && atoi(env) == 1) hint_mode_enabled = true;
        hint_checked = true;
        
        // Init CSV header if first run
        FILE* f = fopen("ground_truth.csv", "w");
        if (f) { fprintf(f, "timestamp,phase\n"); fclose(f); }
    }

    // 2. Change Detection (Don't log if phase hasn't changed)
    if (last_logged_phase == new_phase) return;
    last_logged_phase = new_phase;

    // 3. Batch Logging to RAM
    log_buffer[buffer_count].timestamp = get_epoch_time();
    log_buffer[buffer_count].phase = new_phase;
    buffer_count++;

    // 4. Trigger Flush if Buffer Full
    if (buffer_count >= LOG_BUFFER_SIZE) {
        flush_buffer_to_disk();
    }

    // 5. Shared Memory Hint (Immediate - for your Python Controller)
    if (hint_mode_enabled) {
        FILE* f = fopen("/dev/shm/minimd_phase_hint", "w");
        if (f) {
            fprintf(f, "%s", get_phase_string(new_phase));
            fflush(f);
            fclose(f);
        }
    }
}

// ========================================================================
// SECTION 2: EMBEDDED CHECKPOINT FUNCTIONS
// ========================================================================

// Clean checkpoint directory on rank 0 (remove old checkpoint files)
static void clean_checkpoint_dir(const char* dir) {
  int me = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  if(me != 0) return;

  if(dir == nullptr || dir[0] == '\0') return;

  struct stat st;
  if(stat(dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
    return;
  }

  DIR* dirp = opendir(dir);
  if(dirp == nullptr) {
    std::fprintf(stderr, "WARNING: Could not open checkpoint dir %s for cleanup: %s\n", 
                 dir, std::strerror(errno));
    return;
  }

  struct dirent* entry;
  int removed_count = 0;
  while((entry = readdir(dirp)) != nullptr) {
    if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;

    char filepath[1024];
    snprintf(filepath, sizeof(filepath), "%s/%s", dir, entry->d_name);

    struct stat file_st;
    if(stat(filepath, &file_st) == 0 && S_ISREG(file_st.st_mode)) {
      if(unlink(filepath) == 0) {
        removed_count++;
      }
    }
  }

  closedir(dirp);
  if(removed_count > 0) {
    std::printf("Cleaned %d old checkpoint file(s) from %s/\n", removed_count, dir);
  }
}

// Make directory on rank 0
static void mkdir_rank0(const char* dir) {
  int me = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  if(me != 0) return;

  if(dir == nullptr || dir[0] == '\0') return;

  struct stat st;
  if(stat(dir, &st) == 0 && S_ISDIR(st.st_mode)) return;

  if(mkdir(dir, 0755) != 0 && errno != EEXIST) {
    std::fprintf(stderr, "ERROR: mkdir(%s) failed: %s\n", dir, std::strerror(errno));
  }
}

// End-of-simulation checkpoint function that scales with atom count
static void write_checkpoint_sustained_io(const Atom& atom,
                                          const Comm& comm,
                                          int step,
                                          const char* out_dir,
                                          double target_duration_sec,
                                          size_t chunk_bytes,
                                          int sleep_us,
                                          int fsync_each_chunk)
{
  int me = 0;
  int nprocs = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  const char* dir = (out_dir && out_dir[0]) ? out_dir : "chk";
  
  clean_checkpoint_dir(dir);
  mkdir_rank0(dir);

  MPI_Barrier(MPI_COMM_WORLD);

  double io_start_time = MPI_Wtime();

  char fname[512];
  std::snprintf(fname, sizeof(fname), "%s/checkpoint_step%08d_rank%05d.bin", dir, step, me);

  int fd = ::open(fname, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if(fd < 0) {
    std::fprintf(stderr, "ERROR: cannot open %s: %s\n", fname, std::strerror(errno));
    MPI_Barrier(MPI_COMM_WORLD);
    return;
  }

  // === DATA PREPARATION (CPU Work) ===
  struct CheckpointHeader {
    int32_t nlocal; int32_t nghost; int32_t ntypes;
    int32_t step; int32_t rank; int32_t nprocs;
    double box_xlo, box_xhi, box_ylo, box_yhi, box_zlo, box_zhi;
    double box_xprd, box_yprd, box_zprd;
  } header;

  header.nlocal = atom.nlocal;
  header.nghost = atom.nghost;
  header.ntypes = atom.ntypes;
  header.step = step;
  header.rank = me;
  header.nprocs = nprocs;
  header.box_xlo = atom.box.xlo; header.box_xhi = atom.box.xhi;
  header.box_ylo = atom.box.ylo; header.box_yhi = atom.box.yhi;
  header.box_zlo = atom.box.zlo; header.box_zhi = atom.box.zhi;
  header.box_xprd = atom.box.xprd; header.box_yprd = atom.box.yprd; header.box_zprd = atom.box.zprd;

  std::vector<MMD_float> positions(atom.nlocal * 3);
  std::vector<MMD_float> velocities(atom.nlocal * 3);
  std::vector<MMD_float> forces(atom.nlocal * 3);

  for(int i = 0; i < atom.nlocal; i++) {
    positions[i*3+0] = atom.x[i*PAD+0]; positions[i*3+1] = atom.x[i*PAD+1]; positions[i*3+2] = atom.x[i*PAD+2];
    velocities[i*3+0] = atom.v[i*PAD+0]; velocities[i*3+1] = atom.v[i*PAD+1]; velocities[i*3+2] = atom.v[i*PAD+2];
    forces[i*3+0] = atom.f[i*PAD+0]; forces[i*3+1] = atom.f[i*PAD+1]; forces[i*3+2] = atom.f[i*PAD+2];
  }

  size_t total_data_bytes = sizeof(header) +
                            positions.size() * sizeof(MMD_float) +
                            velocities.size() * sizeof(MMD_float) +
                            forces.size() * sizeof(MMD_float);
  
  if(atom.type) total_data_bytes += static_cast<size_t>(atom.nlocal) * sizeof(int);

  std::vector<unsigned char> all_data;
  all_data.reserve(total_data_bytes);

  const unsigned char* header_ptr = reinterpret_cast<const unsigned char*>(&header);
  all_data.insert(all_data.end(), header_ptr, header_ptr + sizeof(header));

  const unsigned char* pos_ptr = reinterpret_cast<const unsigned char*>(positions.data());
  all_data.insert(all_data.end(), pos_ptr, pos_ptr + positions.size() * sizeof(MMD_float));

  const unsigned char* vel_ptr = reinterpret_cast<const unsigned char*>(velocities.data());
  all_data.insert(all_data.end(), vel_ptr, vel_ptr + velocities.size() * sizeof(MMD_float));

  const unsigned char* force_ptr = reinterpret_cast<const unsigned char*>(forces.data());
  all_data.insert(all_data.end(), force_ptr, force_ptr + forces.size() * sizeof(MMD_float));

  if(atom.type) {
    const unsigned char* type_ptr = reinterpret_cast<const unsigned char*>(atom.type);
    all_data.insert(all_data.end(), type_ptr, type_ptr + atom.nlocal * sizeof(int));
  }

  // === SUSTAINED I/O WRITE ===
  if(chunk_bytes == 0) chunk_bytes = 1024 * 1024;
  if(target_duration_sec <= 0.0) target_duration_sec = 30.0;
  
  size_t bytes_written = 0;
  size_t chunk_count = 0;
  double elapsed = 0.0;

  if(me == 0) {
    std::printf("\n=== Starting Sustained I/O Checkpoint ===\n");
    std::printf("Target Duration: %.1f sec | Data: %.2f MB\n", target_duration_sec, all_data.size()/(1024.0*1024.0));
  }

  // 1. Write Real Data (Fast)
  while(bytes_written < all_data.size() && elapsed < target_duration_sec) {
    size_t bytes_remaining = all_data.size() - bytes_written;
    size_t this_chunk_size = (bytes_remaining < chunk_bytes) ? bytes_remaining : chunk_bytes;

    const unsigned char* write_ptr = all_data.data() + bytes_written;
    size_t written_now = 0;
    while(written_now < this_chunk_size) {
      ssize_t result = ::write(fd, write_ptr + written_now, this_chunk_size - written_now);
      if(result < 0) {
        if(errno == EINTR) continue;
        ::close(fd); MPI_Barrier(MPI_COMM_WORLD); return;
      }
      written_now += static_cast<size_t>(result);
    }
    if(fsync_each_chunk) ::fsync(fd);
    bytes_written += this_chunk_size;
    chunk_count++;
    elapsed = MPI_Wtime() - io_start_time;
  }

  // 2. Write Padding Data (Sustained Load with Sleep)
  if(elapsed < target_duration_sec && bytes_written >= all_data.size()) {
    std::vector<unsigned char> padding_buf(chunk_bytes);
    // Fill pattern
    for(size_t i=0; i<padding_buf.size(); i++) padding_buf[i] = (unsigned char)((i+me)&0xFF);

    while(elapsed < target_duration_sec) {
      size_t remaining = padding_buf.size();
      const unsigned char* pad_ptr = padding_buf.data();
      while(remaining > 0) {
        ssize_t result = ::write(fd, pad_ptr, remaining);
        if(result < 0) { if(errno == EINTR) continue; break; }
        pad_ptr += result; remaining -= result;
      }
      if(fsync_each_chunk) ::fsync(fd);
      bytes_written += chunk_bytes;
      chunk_count++;
      
      if(sleep_us > 0) ::usleep(sleep_us);
      elapsed = MPI_Wtime() - io_start_time;
    }
  }

  ::fsync(fd);
  ::close(fd);

  if(me == 0) {
    double total_mb = static_cast<double>(bytes_written) / (1024.0 * 1024.0);
    std::printf("\n=== I/O Complete: %.2f MB written in %.2f s ===\n", total_mb, MPI_Wtime() - io_start_time);
  }
  
  MPI_Barrier(MPI_COMM_WORLD);
  (void)comm;
}

// ========================================================================
// SECTION 3: INTEGRATE CLASS METHODS
// ========================================================================

Integrate::Integrate() {
  sort_every=20;
  
  // --- Checkpoint Configuration Init ---
  ckpt_interval = 0;       
  ckpt_dir = "chk";       
  ckpt_at_end = 1;         // Enable end-of-sim checkpoint
  
  // Sustained I/O defaults
  ckpt_io_duration_sec = 30.0;
  ckpt_chunk_bytes = 1024 * 1024;
  ckpt_sleep_us = 100000; // 100 ms
  ckpt_fsync_chunks = 0;
}

Integrate::~Integrate() { 
    // Flush logs on exit
    int mpi_initialized = 0;
    int mpi_finalized = 0;
    MPI_Initialized(&mpi_initialized);
    MPI_Finalized(&mpi_finalized);

    if (mpi_initialized && !mpi_finalized) {
        int my_rank;
        MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
        if(my_rank == 0) flush_buffer_to_disk();
    }
}

void Integrate::setup()
{
  dtforce = 0.5 * dt;
}

void Integrate::initialIntegrate()
{
  OMPFORSCHEDULE
  for(MMD_int i = 0; i < nlocal; i++) {
    v[i * PAD + 0] += dtforce * f[i * PAD + 0];
    v[i * PAD + 1] += dtforce * f[i * PAD + 1];
    v[i * PAD + 2] += dtforce * f[i * PAD + 2];
    x[i * PAD + 0] += dt * v[i * PAD + 0];
    x[i * PAD + 1] += dt * v[i * PAD + 1];
    x[i * PAD + 2] += dt * v[i * PAD + 2];
  }
}

void Integrate::finalIntegrate()
{
  OMPFORSCHEDULE
  for(MMD_int i = 0; i < nlocal; i++) {
    v[i * PAD + 0] += dtforce * f[i * PAD + 0];
    v[i * PAD + 1] += dtforce * f[i * PAD + 1];
    v[i * PAD + 2] += dtforce * f[i * PAD + 2];
  }
}

// ========================================================================
// SECTION 4: MAIN RUN LOOP (With Instrumentation & Checkpointing)
// ========================================================================

void Integrate::run(Atom &atom, Force* force, Neighbor &neighbor,
                    Comm &comm, Thermo &thermo, Timer &timer)
{
  int i, n;
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

  // --- [PHASE: SERIAL COMPUTE (Setup)] ---
  update_phase(PHASE_SERIAL_COMPUTE, my_rank);

  comm.timer = &timer;
  timer.array[TIME_TEST] = 0.0;
  int check_safeexchange = comm.check_safeexchange;
  mass = atom.mass;
  dtforce = dtforce / mass;

  #pragma omp parallel private(i,n)
  {
    int next_sort = sort_every>0?sort_every:ntimes+1;

    for(n = 0; n < ntimes; n++) {

      #pragma omp barrier

      // --- [PHASE: PARALLEL COMPUTE] ---
      #pragma omp master 
      { update_phase(PHASE_PARALLEL_COMPUTE, my_rank); }
      
      x = atom.x; v = atom.v; f = atom.f; xold = atom.xold; nlocal = atom.nlocal;
      initialIntegrate();

      #pragma omp master
      timer.stamp();

      if((n + 1) % neighbor.every) {
        
        // --- [PHASE: COMMUNICATION] ---
        #pragma omp master
        { update_phase(PHASE_COMMUNICATION, my_rank); }

        comm.communicate(atom);
        
        #pragma omp master
        timer.stamp(TIME_COMM);

      } else {
          if(check_safeexchange) {
            #pragma omp master
            {
               double d_max = 0;
               for(i = 0; i < atom.nlocal; i++) {
                 double dx = (x[i * PAD + 0] - xold[i * PAD + 0]);
                 if(dx > atom.box.xprd) dx -= atom.box.xprd;
                 if(dx < -atom.box.xprd) dx += atom.box.xprd;
                 double dy = (x[i * PAD + 1] - xold[i * PAD + 1]);
                 if(dy > atom.box.yprd) dy -= atom.box.yprd;
                 if(dy < -atom.box.yprd) dy += atom.box.yprd;
                 double dz = (x[i * PAD + 2] - xold[i * PAD + 2]);
                 if(dz > atom.box.zprd) dz -= atom.box.zprd;
                 if(dz < -atom.box.zprd) dz += atom.box.zprd;
                 double d = dx * dx + dy * dy + dz * dz;
                 if(d > d_max) d_max = d;
               }
               d_max = sqrt(d_max);
               if((d_max > atom.box.xhi - atom.box.xlo) || (d_max > atom.box.yhi - atom.box.ylo) || (d_max > atom.box.zhi - atom.box.zlo))
                 printf("Warning: Atoms move further than your subdomain size...\n");
            }
          }

          // --- [PHASE: COMMUNICATION (Exchange/Sort)] ---
          #pragma omp master
          { update_phase(PHASE_COMMUNICATION, my_rank); }

          #pragma omp master
          timer.stamp_extra_start();
          
          comm.exchange(atom);
          if(n+1>=next_sort) {
            atom.sort(neighbor);
            next_sort +=  sort_every;
          }
          comm.borders(atom);
          
          #pragma omp master
          {
            timer.stamp_extra_stop(TIME_TEST);
            timer.stamp(TIME_COMM);
          }

          if(check_safeexchange)
            for(int i = 0; i < PAD * atom.nlocal; i++) xold[i] = x[i];
        
        #pragma omp barrier

        // --- [PHASE: MEMORY BOUND (Neighbor Build - Cache Aware)] ---
        // HEURISTIC: Check if data fits in typical L3 Cache (30MB)
        #pragma omp master 
        {
             long data_size_bytes = (atom.nlocal + atom.nghost) * 3 * sizeof(MMD_float);
             // UPDATED: 60MB Threshold based on hardware (64MB L3)
             long l3_cache_limit = 60L * 1024L * 1024L;
             
             if (data_size_bytes > l3_cache_limit) {
                 update_phase(PHASE_MEMORY_BOUND, my_rank); 
             } else {
                 // Fits in Cache -> Acts like Compute -> Flag as Parallel Compute
                 update_phase(PHASE_PARALLEL_COMPUTE, my_rank);
             }
        }

        neighbor.build(atom);

        #pragma omp master
        timer.stamp(TIME_NEIGH);
      }

      force->evflag = (n + 1) % thermo.nstat == 0;

      // --- [PHASE: PARALLEL COMPUTE (Force Calc)] ---
      #pragma omp master
      { update_phase(PHASE_PARALLEL_COMPUTE, my_rank); }

      force->compute(atom, neighbor, comm, comm.me);

      #pragma omp master
      timer.stamp(TIME_FORCE);

      if(neighbor.halfneigh && neighbor.ghost_newton) {
        
        // --- [PHASE: COMMUNICATION (Reverse)] ---
        #pragma omp master
        { update_phase(PHASE_COMMUNICATION, my_rank); }

        comm.reverse_communicate(atom);

        #pragma omp master
        timer.stamp(TIME_COMM);
      }

      v = atom.v; f = atom.f; nlocal = atom.nlocal;

      // Log parallel compute before barrier/final integrate
      #pragma omp master
      { update_phase(PHASE_PARALLEL_COMPUTE, my_rank); }

      #pragma omp barrier
      finalIntegrate();

      if(thermo.nstat) thermo.compute(n + 1, atom, neighbor, force, timer, comm);
    }
  } // End OpenMP parallel region

  // --- [PHASE: SERIAL COMPUTE (Cleanup)] ---
  update_phase(PHASE_SERIAL_COMPUTE, my_rank);
  
  // -------------------------------------------------------
  // END-OF-SIMULATION CHECKPOINT (Merged)
  // -------------------------------------------------------
  if(ckpt_at_end) {
    if(my_rank == 0) {
      std::printf("\n========================================\n");
      std::printf("Performing end-of-simulation checkpoint with sustained I/O\n");
      std::printf("========================================\n");
    }
    
    // --- [CRITICAL: Flag this as IO Phase for monitor] ---
    update_phase(PHASE_IO, my_rank);

    write_checkpoint_sustained_io(atom, comm, ntimes, ckpt_dir,
                                  ckpt_io_duration_sec,
                                  ckpt_chunk_bytes,
                                  ckpt_sleep_us,
                                  ckpt_fsync_chunks);
                                  
    // Flag as finished after IO
    update_phase(PHASE_FINISHED, my_rank);
  }

  if (my_rank == 0) {
      flush_buffer_to_disk();
  }
}
