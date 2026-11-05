#include "atom.h"
#include "comm.h"
#include "force.h"
#include "neighbor.h"
#include "thermo.h"
#include "threadData.h"
#include "timer.h"


#include <cstddef>

class Integrate {
public:
  MMD_float dt;
  MMD_float dtforce;
  MMD_int ntimes;
  MMD_int nlocal, nmax;
  MMD_float *x, *v, *f, *xold;
  MMD_float mass;

  MMD_int sort_every;

  // Checkpoint configuration
  MMD_int ckpt_interval; // DEPRECATED - kept for compatibility
  const char *ckpt_dir;  // Output directory for checkpoint files
  MMD_int ckpt_at_end;   // Perform checkpoint at end of simulation (default: 1)

  // Sustained I/O parameters (for monitoring I/O effects)
  double ckpt_io_duration_sec; // Target I/O duration in seconds (default: 30.0)
  size_t ckpt_chunk_bytes;     // Bytes per chunk write (default: 1 MB)
  int ckpt_sleep_us; // Sleep between chunks in microseconds (default: 100ms)
  int ckpt_fsync_chunks; // Whether to fsync after each chunk (default: 0)

  // Communication phase parameters (Johnnie)
  int comm_phase_enabled;    // Enable network communication phase (default: 1)
  size_t comm_standin_bytes; // Stand-in total bytes if runtime calc not
                             // available (default: 309 MB)
  int comm_chunk_kb; // Chunk size for MPI_Send/Recv in KB (default: 1024 = 1MB)

  Integrate();
  ~Integrate();
  void setup();
  void initialIntegrate();
  void finalIntegrate();
  void run(Atom &, Force *, Neighbor &, Comm &, Thermo &, Timer &);

  ThreadData *threads;
};
