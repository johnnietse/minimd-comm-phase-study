/* ----------------------------------------------------------------------
   Communication Monitor for miniMD
   Provides detailed monitoring of communication patterns and performance
---------------------------------------------------------------------- */

#ifndef COMM_MONITOR_H
#define COMM_MONITOR_H

#include "mpi.h"
#include "comm.h"
#include "atom.h"
#include "timer.h"
#include <vector>
#include <map>
#include <string>

class CommMonitor {
public:
    CommMonitor();
    ~CommMonitor();
    
    void start_monitoring();
    void stop_monitoring();
    
    // Monitor specific communication phases
    void record_comm_phase(const std::string& phase_name, int bytes_sent, int bytes_received, 
                          double start_time, double end_time, int dest_rank, int src_rank);
    void record_exchange_phase(int atoms_sent, int atoms_received, double duration);
    void record_border_phase(int ghosts_sent, int ghosts_received, double duration);
    
    // Analysis functions
    void generate_comm_report();
    void analyze_communication_patterns();
    void validate_communication_quality(Atom &atom);
    
    // Real-time monitoring
    void enable_realtime_monitoring(bool enable);
    
private:
    struct CommEvent {
        std::string phase;
        double start_time;
        double end_time;
        int bytes_sent;
        int bytes_received;
        int dest_rank;
        int src_rank;
        bool completed;
    };
    
    std::vector<CommEvent> comm_events_;
    std::map<std::string, double> phase_timings_;
    std::map<std::string, long long> phase_volumes_;
    
    bool monitoring_enabled_;
    bool realtime_monitoring_;
    int my_rank_;
    int num_procs_;
    
    void calculate_communication_efficiency();
    void detect_communication_imbalance();
    void verify_ghost_atom_consistency(Atom &atom);
};

#endif
