/* ----------------------------------------------------------------------
   Communication Monitor Implementation
---------------------------------------------------------------------- */

#include "comm_monitor.h"
#include "stdio.h"
#include "stdlib.h"
#include "math.h"
#include "mpi.h"

CommMonitor::CommMonitor() {
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank_);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs_);
    monitoring_enabled_ = false;
    realtime_monitoring_ = false;
}

CommMonitor::~CommMonitor() {
    // Cleanup if needed
}

void CommMonitor::start_monitoring() {
    monitoring_enabled_ = true;
    comm_events_.clear();
    phase_timings_.clear();
    phase_volumes_.clear();
    
    if (my_rank_ == 0) {
        printf("# Communication monitoring started\n");
    }
}

void CommMonitor::stop_monitoring() {
    monitoring_enabled_ = false;
    
    if (my_rank_ == 0) {
        printf("# Communication monitoring stopped\n");
    }
}

void CommMonitor::record_comm_phase(const std::string& phase_name, int bytes_sent, 
                                   int bytes_received, double start_time, 
                                   double end_time, int dest_rank, int src_rank) {
    if (!monitoring_enabled_) return;
    
    CommEvent event;
    event.phase = phase_name;
    event.start_time = start_time;
    event.end_time = end_time;
    event.bytes_sent = bytes_sent;
    event.bytes_received = bytes_received;
    event.dest_rank = dest_rank;
    event.src_rank = src_rank;
    event.completed = true;
    
    comm_events_.push_back(event);
    
    // Update phase statistics
    double duration = end_time - start_time;
    phase_timings_[phase_name] += duration;
    phase_volumes_[phase_name] += bytes_sent + bytes_received;
    
    // Real-time output
    if (realtime_monitoring_ && my_rank_ == 0) {
        printf("COMM_EVENT: Phase=%s Duration=%.6f BytesSent=%d BytesReceived=%d\n",
               phase_name.c_str(), duration, bytes_sent, bytes_received);
    }
}

void CommMonitor::record_exchange_phase(int atoms_sent, int atoms_received, double duration) {
    if (!monitoring_enabled_) return;
    
    int bytes_per_atom = 7 * sizeof(MMD_float); // Based on pack_exchange
    int bytes_sent = atoms_sent * bytes_per_atom;
    int bytes_received = atoms_received * bytes_per_atom;
    
    record_comm_phase("exchange", bytes_sent, bytes_received, 
                     MPI_Wtime() - duration, MPI_Wtime(), -1, -1);
}

void CommMonitor::record_border_phase(int ghosts_sent, int ghosts_received, double duration) {
    if (!monitoring_enabled_) return;
    
    int bytes_per_ghost = 4 * sizeof(MMD_float); // Based on pack_border
    int bytes_sent = ghosts_sent * bytes_per_ghost;
    int bytes_received = ghosts_received * bytes_per_ghost;
    
    record_comm_phase("border", bytes_sent, bytes_received, 
                     MPI_Wtime() - duration, MPI_Wtime(), -1, -1);
}

void CommMonitor::generate_comm_report() {
    if (my_rank_ != 0) return;
    
    printf("\n# ==========================================\n");
    printf("# COMMUNICATION MONITORING REPORT\n");
    printf("# ==========================================\n");
    
    // Aggregate statistics across all processes
    std::map<std::string, double> global_timings;
    std::map<std::string, long long> global_volumes;
    
    for (const auto& phase : phase_timings_) {
        double global_time;
        MPI_Reduce(&phase_timings_[phase.first], &global_time, 1, MPI_DOUBLE, 
                  MPI_SUM, 0, MPI_COMM_WORLD);
        global_timings[phase.first] = global_time / num_procs_;
        
        long long global_volume;
        MPI_Reduce(&phase_volumes_[phase.first], &global_volume, 1, MPI_LONG_LONG,
                  MPI_SUM, 0, MPI_COMM_WORLD);
        global_volumes[phase.first] = global_volume;
    }
    
    // Print report
    printf("# Phase               Time(s)    Data(MB)    BW(MB/s)\n");
    printf("# ---------------------------------------------------\n");
    
    for (const auto& phase : global_timings) {
        double time = phase.second;
        double data_mb = global_volumes[phase.first] / (1024.0 * 1024.0);
        double bandwidth = (time > 0) ? data_mb / time : 0.0;
        
        printf("# %-18s %9.4f %11.2f %11.2f\n", 
               phase.first.c_str(), time, data_mb, bandwidth);
    }
    
    // Communication efficiency analysis
    analyze_communication_patterns();
}

void CommMonitor::analyze_communication_patterns() {
    if (my_rank_ != 0) return;
    
    printf("\n# COMMUNICATION PATTERN ANALYSIS:\n");
    
    // Calculate communication efficiency metrics
    double total_comm_time = 0.0;
    for (const auto& phase : phase_timings_) {
        total_comm_time += phase_timings_[phase.first];
    }
    
    // Detect potential issues
    if (phase_timings_["exchange"] > phase_timings_["border"] * 2.0) {
        printf("# WARNING: Exchange communication takes significantly longer than border communication\n");
        printf("#          Consider adjusting domain decomposition or neighbor list settings\n");
    }
    
    if (total_comm_time > 0.3 * phase_timings_["total"]) { // Assuming total time is tracked
        printf("# WARNING: Communication overhead > 30%% of total time\n");
        printf("#          May indicate load imbalance or network issues\n");
    }
}

void CommMonitor::validate_communication_quality(Atom &atom) {
    // Verify ghost atom consistency
    verify_ghost_atom_consistency(atom);
    
    // Check for communication errors
    int local_error = 0;
    int global_error = 0;
    
    // Validate that all expected ghost atoms are present
    if (atom.nghost < 0) {
        local_error = 1;
    }
    
    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    
    if (global_error && my_rank_ == 0) {
        printf("# ERROR: Communication validation failed - inconsistent ghost atoms\n");
    }
}

void CommMonitor::verify_ghost_atom_consistency(Atom &atom) {
    // Basic sanity checks for ghost atoms
    int error = 0;
    
    for (int i = atom.nlocal; i < atom.nlocal + atom.nghost; i++) {
        // Check if ghost atom coordinates are within expected bounds
        if (atom.x[i * PAD + 0] < atom.box.xlo - atom.box.xprd || 
            atom.x[i * PAD + 0] > atom.box.xhi + atom.box.xprd ||
            atom.x[i * PAD + 1] < atom.box.ylo - atom.box.yprd || 
            atom.x[i * PAD + 1] > atom.box.yhi + atom.box.yprd ||
            atom.x[i * PAD + 2] < atom.box.zlo - atom.box.zprd || 
            atom.x[i * PAD + 2] > atom.box.zhi + atom.box.zprd) {
            error = 1;
            break;
        }
    }
    
    int global_error;
    MPI_Allreduce(&error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    
    if (global_error && my_rank_ == 0) {
        printf("# WARNING: Ghost atoms outside expected bounds detected\n");
    }
}

void CommMonitor::enable_realtime_monitoring(bool enable) {
    realtime_monitoring_ = enable;
    if (my_rank_ == 0) {
        printf("# Real-time communication monitoring: %s\n", 
               enable ? "ENABLED" : "DISABLED");
    }
}
