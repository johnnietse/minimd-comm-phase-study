/* ----------------------------------------------------------------------
   Instrumented Communication class with monitoring capabilities
---------------------------------------------------------------------- */

#ifndef COMM_INSTRUMENTED_H
#define COMM_INSTRUMENTED_H

#include "comm.h"
#include "comm_monitor.h"

class CommInstrumented : public Comm {
public:
    CommInstrumented();
    ~CommInstrumented();
    
    // Override communication methods with monitoring
    void communicate(Atom &atom) override;
    void reverse_communicate(Atom &atom) override;
    void exchange(Atom &atom) override;
    void borders(Atom &atom) override;
    
    // Monitoring control
    void enable_monitoring(bool enable);
    void generate_report();
    void validate_communication(Atom &atom);
    
private:
    CommMonitor monitor_;
    bool monitoring_enabled_;
    
    void monitor_communicate(Atom &atom);
    void monitor_exchange(Atom &atom);
    void monitor_borders(Atom &atom);
};

#endif
