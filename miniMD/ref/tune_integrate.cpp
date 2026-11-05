/* ----------------------------------------------------------------------
   miniMD is a simple, parallel molecular dynamics (MD) code.   miniMD is
   an MD microapplication in the Mantevo project at Sandia National
   Laboratories ( http://www.mantevo.org ). The primary
   authors of miniMD are Steve Plimpton (sjplimp@sandia.gov) , Paul Crozier
   (pscrozi@sandia.gov) and Christian Trott (crtrott@sandia.gov).

   Copyright (2008) Sandia Corporation.  Under the terms of Contract
   DE-AC04-94AL85000 with Sandia Corporation, the U.S. Government retains
   certain rights in this software.  This library is free software; you
   can redistribute it and/or modify it under the terms of the GNU Lesser
   General Public License as published by the Free Software Foundation;
   either version 3 of the License, or (at your option) any later
   version.

   This library is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with this software; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
   USA.  See also: http://www.gnu.org/licenses/lgpl.txt .

   For questions, contact Paul S. Crozier (pscrozi@sandia.gov) or
   Christian Trott (crtrott@sandia.gov).

   Please read the accompanying README and LICENSE files.
---------------------------------------------------------------------- */
//#define PRINTDEBUG(a) a
#define PRINTDEBUG(a)
#include "stdio.h"
#include "integrate.h"
#include "openmp.h"
#include "math.h"

// --- [INSTRUMENTATION START] ---
#include <sys/time.h>
#include <mpi.h>

static FILE* truth_log = NULL;

// Helper to get Unix Epoch time with microsecond precision
double get_epoch_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}
// --- [INSTRUMENTATION END] ---

Integrate::Integrate() {sort_every=20;}
Integrate::~Integrate() {
  // Optional: Close file if object is destroyed, though OS handles this on exit usually
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

  // --- [INSTRUMENTATION START] ---
  // Initialize the ground truth log file on Rank 0 only
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  if (my_rank == 0 && truth_log == NULL) {
      truth_log = fopen("ground_truth.csv", "w");
      if (truth_log) {
          fprintf(truth_log, "timestamp,actual_phase\n");
          fflush(truth_log);
      }
  }
  // --- [INSTRUMENTATION END] ---

  comm.timer = &timer;
  timer.array[TIME_TEST] = 0.0;

  int check_safeexchange = comm.check_safeexchange;

  mass = atom.mass;
  dtforce = dtforce / mass;
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

      initialIntegrate();

      #pragma omp master
      timer.stamp();

      if((n + 1) % neighbor.every) {

        // --- [INSTRUMENTATION] Start COMMUNICATION ---
        #pragma omp master
        if(truth_log) { fprintf(truth_log, "%.6f,COMMUNICATION\n", get_epoch_time()); fflush(truth_log); }

        comm.communicate(atom);
        #pragma omp master
        timer.stamp(TIME_COMM);

      } else {
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
                printf("Warning: Atoms move further than your subdomain size, which will eventually cause lost atoms.\n"
                "Increase reneighboring frequency or choose a different processor grid\n"
                "Maximum move distance: %lf; Subdomain dimensions: %lf %lf %lf\n",
                d_max, atom.box.xhi - atom.box.xlo, atom.box.yhi - atom.box.ylo, atom.box.zhi - atom.box.zlo);

            }

          }

          // --- [INSTRUMENTATION] Start COMMUNICATION (Exchange/Sort) ---
          #pragma omp master
          if(truth_log) { fprintf(truth_log, "%.6f,COMMUNICATION\n", get_epoch_time()); fflush(truth_log); }

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

        #pragma omp barrier

        // --- [INSTRUMENTATION] Start MEMORY_BOUND (Neighbor Build) ---
        #pragma omp master
        if(truth_log) { fprintf(truth_log, "%.6f,MEMORY_BOUND\n", get_epoch_time()); fflush(truth_log); }

        neighbor.build(atom);

        // #pragma omp barrier

        #pragma omp master
        timer.stamp(TIME_NEIGH);
      }

      force->evflag = (n + 1) % thermo.nstat == 0;

      // --- [INSTRUMENTATION] Start COMPUTE (Force) ---
      #pragma omp master
      if(truth_log) { fprintf(truth_log, "%.6f,COMPUTE\n", get_epoch_time()); fflush(truth_log); }

      force->compute(atom, neighbor, comm, comm.me);

      #pragma omp master
      timer.stamp(TIME_FORCE);

      if(neighbor.halfneigh && neighbor.ghost_newton) {
        
        // --- [INSTRUMENTATION] Start COMMUNICATION (Reverse) ---
        #pragma omp master
        if(truth_log) { fprintf(truth_log, "%.6f,COMMUNICATION\n", get_epoch_time()); fflush(truth_log); }

        comm.reverse_communicate(atom);

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
