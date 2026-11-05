/* ----------------------------------------------------------------------
   miniMD is a simple, parallel molecular dynamics (MD) code.
   ... (Header comments kept the same) ...
---------------------------------------------------------------------- */
//#define PRINTDEBUG(a) a
#define PRINTDEBUG(a)
#include "stdio.h"
#include "integrate.h"
#include "openmp.h"
#include "math.h"
#include <mpi.h> // --- [INSTRUMENTATION] Added for timestamps ---

// --- [INSTRUMENTATION] Global File Handle for Ground Truth ---
static FILE* truth_log = NULL;

Integrate::Integrate() {sort_every=20;}
Integrate::~Integrate() {
    // Close log if open
    if (truth_log) fclose(truth_log);
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

void Integrate::run(Atom &atom, Force* force, Neighbor &neighbor,
                    Comm &comm, Thermo &thermo, Timer &timer)
{
  int i, n;

  comm.timer = &timer;
  timer.array[TIME_TEST] = 0.0;

  int check_safeexchange = comm.check_safeexchange;

  mass = atom.mass;
  dtforce = dtforce / mass;

  // --- [INSTRUMENTATION] Initialize Log File (Once per Rank) ---
  if (truth_log == NULL) {
      int rank;
      MPI_Comm_rank(MPI_COMM_WORLD, &rank);
      char filename[64];
      sprintf(filename, "truth_rank_%d.csv", rank);
      truth_log = fopen(filename, "w");
      if (truth_log) fprintf(truth_log, "Timestamp,Phase,Event\n");
  }

  //Use OpenMP threads only within the following loop containing the main loop.
  //Do not use OpenMP for setup and postprocessing.
  #pragma omp parallel private(i,n)
  {
    int next_sort = sort_every>0?sort_every:ntimes+1;

    for(n = 0; n < ntimes; n++) {

      #pragma omp barrier

      x = atom.x;
      v = atom.v;
      f = atom.f;
      xold = atom.xold;
      nlocal = atom.nlocal;

      // Note: initialIntegrate is very fast, usually not worth logging as a phase
      initialIntegrate();

      #pragma omp master
      timer.stamp();

      if((n + 1) % neighbor.every) {

        // --- [INSTRUMENTATION] Start COMM Phase (Standard Ghost Update) ---
        #pragma omp master
        if(truth_log) fprintf(truth_log, "%.6f,COMMUNICATION,START\n", MPI_Wtime());

        comm.communicate(atom);

        // --- [INSTRUMENTATION] End COMM Phase ---
        #pragma omp master
        if(truth_log) fprintf(truth_log, "%.6f,COMMUNICATION,END\n", MPI_Wtime());

        #pragma omp master
        timer.stamp(TIME_COMM);

      } else {
        // --- [INSTRUMENTATION] Start COMM Phase (Reneighboring Exchange) ---
        #pragma omp master
        if(truth_log) fprintf(truth_log, "%.6f,COMMUNICATION,START\n", MPI_Wtime());

        //these routines are not yet ported to OpenMP
        {
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
        }

        // --- [INSTRUMENTATION] End COMM Phase ---
        #pragma omp master
        if(truth_log) fprintf(truth_log, "%.6f,COMMUNICATION,END\n", MPI_Wtime());

        #pragma omp barrier

        // --- [INSTRUMENTATION] Start MEMORY BOUND Phase ---
        #pragma omp master
        if(truth_log) fprintf(truth_log, "%.6f,MEMORY_BOUND,START\n", MPI_Wtime());

        neighbor.build(atom);

        // --- [INSTRUMENTATION] End MEMORY BOUND Phase ---
        #pragma omp master
        if(truth_log) fprintf(truth_log, "%.6f,MEMORY_BOUND,END\n", MPI_Wtime());

        // #pragma omp barrier

        #pragma omp master
        timer.stamp(TIME_NEIGH);
      }

      force->evflag = (n + 1) % thermo.nstat == 0;

      // --- [INSTRUMENTATION] Start COMPUTE Phase ---
      #pragma omp master
      if(truth_log) fprintf(truth_log, "%.6f,COMPUTE,START\n", MPI_Wtime());

      force->compute(atom, neighbor, comm, comm.me);

      // --- [INSTRUMENTATION] End COMPUTE Phase ---
      #pragma omp master
      if(truth_log) fprintf(truth_log, "%.6f,COMPUTE,END\n", MPI_Wtime());

      #pragma omp master
      timer.stamp(TIME_FORCE);

      if(neighbor.halfneigh && neighbor.ghost_newton) {
        
        // --- [INSTRUMENTATION] Start COMM Phase (Reverse) ---
        #pragma omp master
        if(truth_log) fprintf(truth_log, "%.6f,COMMUNICATION,START\n", MPI_Wtime());

        comm.reverse_communicate(atom);

        // --- [INSTRUMENTATION] End COMM Phase (Reverse) ---
        #pragma omp master
        if(truth_log) fprintf(truth_log, "%.6f,COMMUNICATION,END\n", MPI_Wtime());

        #pragma omp master
        timer.stamp(TIME_COMM);
      }

      v = atom.v;
      f = atom.f;
      nlocal = atom.nlocal;

      #pragma omp barrier

      finalIntegrate();

      if(thermo.nstat) thermo.compute(n + 1, atom, neighbor, force, timer, comm);

    }
  } //end OpenMP parallel
}
