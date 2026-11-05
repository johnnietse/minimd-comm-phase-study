/* ----------------------------------------------------------------------
   Instrumented Communication Implementation
---------------------------------------------------------------------- */

#include "comm_instrumented.h"
#include "mpi.h"
#include "openmp.h"

CommInstrumented::CommInstrumented() : Comm() {
    monitoring_enabled_ = false;
}

CommInstrumented::~CommInstrumented() {
    // Cleanup
}

void CommInstrumented::enable_monitoring(bool enable) {
    monitoring_enabled_ = enable;
    if (enable) {
        monitor_.start_monitoring();
        monitor_.enable_realtime_monitoring(true);
    } else {
        monitor_.stop_monitoring();
    }
}

void CommInstrumented::communicate(Atom &atom) {
    if (monitoring_enabled_) {
        monitor_communicate(atom);
    } else {
        Comm::communicate(atom);
    }
}

void CommInstrumented::monitor_communicate(Atom &atom) {
    double start_time = MPI_Wtime();
    
    // Call original communicate
    Comm::communicate(atom);
    
    double end_time = MPI_Wtime();
    double duration = end_time - start_time;
    
    // Estimate communication volume
    int total_bytes = 0;
    for (int iswap = 0; iswap < nswap; iswap++) {
        total_bytes += comm_send_size[iswap] * sizeof(MMD_float);
        total_bytes += comm_recv_size[iswap] * sizeof(MMD_float);
    }
    
    monitor_.record_comm_phase("communicate", total_bytes/2, total_bytes/2, 
                              start_time, end_time, -1, -1);
}

void CommInstrumented::exchange(Atom &atom) {
    if (monitoring_enabled_) {
        monitor_exchange(atom);
    } else {
        Comm::exchange(atom);
    }
}

void CommInstrumented::monitor_exchange(Atom &atom) {
    double start_time = MPI_Wtime();
    int initial_nlocal = atom.nlocal;
    
    // Call original exchange
    Comm::exchange(atom);
    
    double end_time = MPI_Wtime();
    double duration = end_time - start_time;
    
    // Calculate atoms exchanged
    int atoms_sent = initial_nlocal - atom.nlocal;
    int atoms_received = atom.nlocal - initial_nlocal;
    
    monitor_.record_exchange_phase(atoms_sent, atoms_received, duration);
}

void CommInstrumented::borders(Atom &atom) {
    if (monitoring_enabled_) {
        monitor_borders(atom);
    } else {
        Comm::borders(atom);
    }
}

void CommInstrumented::monitor_borders(Atom &atom) {
    double start_time = MPI_Wtime();
    int initial_nghost = atom.nghost;
    
    // Call original borders
    Comm::borders(atom);
    
    double end_time = MPI_Wtime();
    double duration = end_time - start_time;
    
    // Calculate ghost atoms exchanged
    int ghosts_received = atom.nghost - initial_nghost;
    
    // Estimate ghosts sent (simplified)
    int ghosts_sent = 0;
    for (int iswap = 0; iswap < nswap; iswap++) {
        ghosts_sent += sendnum[iswap];
    }
    
    monitor_.record_border_phase(ghosts_sent, ghosts_received, duration);
}

void CommInstrumented::reverse_communicate(Atom &atom) {
    double start_time = MPI_Wtime();
    
    // Call original reverse_communicate
    Comm::reverse_communicate(atom);
    
    double end_time = MPI_Wtime();
    
    if (monitoring_enabled_) {
        int total_bytes = 0;
        for (int iswap = 0; iswap < nswap; iswap++) {
            total_bytes += reverse_send_size[iswap] * sizeof(MMD_float);
            total_bytes += reverse_recv_size[iswap] * sizeof(MMD_float);
        }
        
        monitor_.record_comm_phase("reverse_communicate", total_bytes/2, total_bytes/2, 
                                  start_time, end_time, -1, -1);
    }
}

void CommInstrumented::generate_report() {
    if (monitoring_enabled_) {
        monitor_.generate_comm_report();
    }
}

void CommInstrumented::validate_communication(Atom &atom) {
    if (monitoring_enabled_) {
        monitor_.validate_communication_quality(atom);
    }
}
