import os
import time
import struct
import ctypes
from ctypes import c_int, c_uint32, c_uint64, Structure, POINTER, byref

# --- CONFIGURATION ---
# UPDATE THESE TO THE SPECIFIC 8 CORES YOU OWN
MY_CORES = [0, 1, 2, 3, 4, 5, 6, 7] 

# Frequency Steps (in kHz) based on your EPYC 7551P
FREQ_RACE = 2400000  # 2.4 GHz (Boost/High)
FREQ_MEM  = 1600000  # 1.6 GHz (Memory Bound)
FREQ_IDLE = 1200000  # 1.2 GHz (Spinning/Idle)

# --- LIBC & SYSCALL SETUP ---
libc = ctypes.CDLL(None, use_errno=True)
syscall = libc.syscall
__NR_perf_event_open = 298

# --- PERF CONSTANTS (From linux/perf_event.h) ---
PERF_TYPE_HARDWARE = 0
PERF_TYPE_HW_CACHE = 3

PERF_COUNT_HW_CPU_CYCLES = 0
PERF_COUNT_HW_INSTRUCTIONS = 1

# LLC (L3) Read Miss Config
# (ID=2) | (READ=0)<<8 | (MISS=1)<<16
LLC_READ_MISS_CONFIG = (2) | (0 << 8) | (1 << 16)

class PerfEventAttr(Structure):
    _fields_ = [
        ("type", c_uint32),
        ("size", c_uint32),
        ("config", c_uint64),
        ("sample_period", c_uint64),
        ("sample_type", c_uint64),
        ("read_format", c_uint64),
        ("disabled", c_uint64, 1),
        ("inherit", c_uint64, 1),
        ("pinned", c_uint64, 1),
        ("exclusive", c_uint64, 1),
        ("exclude_user", c_uint64, 1),
        ("exclude_kernel", c_uint64, 1), # EXCLUDE KERNEL to focus on App
        ("exclude_hv", c_uint64, 1),
        ("exclude_idle", c_uint64, 1),
        ("mmap", c_uint64, 1),
        ("comm", c_uint64, 1),
        ("freq", c_uint64, 1),
        ("inherit_stat", c_uint64, 1),
        ("enable_on_exec", c_uint64, 1),
        ("task", c_uint64, 1),
        ("watermark", c_uint64, 1),
        ("precise_ip", c_uint64, 2),
        ("mmap_data", c_uint64, 1),
        ("sample_id_all", c_uint64, 1),
        ("exclude_host", c_uint64, 1),
        ("exclude_guest", c_uint64, 1),
        ("exclude_callchain_kernel", c_uint64, 1),
        ("exclude_callchain_user", c_uint64, 1),
        ("mmap2", c_uint64, 1),
        ("comm_exec", c_uint64, 1),
        ("use_clockid", c_uint64, 1),
        ("context_switch", c_uint64, 1),
        ("write_backward", c_uint64, 1),
        ("namespaces", c_uint64, 1),
        ("reserved", c_uint64, 35)
    ]

def perf_event_open(attr, pid, cpu, group_fd, flags):
    attr.size = ctypes.sizeof(attr)
    return syscall(__NR_perf_event_open, byref(attr), c_int(pid), c_int(cpu), c_int(group_fd), c_uint64(flags))

class CoreController:
    def __init__(self, core_id):
        self.core_id = core_id
        self.fds = {}
        self.prev = {}
        self.current_freq = -1
        self.scaling_path = f"/sys/devices/system/cpu/cpu{core_id}/cpufreq/scaling_setspeed"
        
        # 1. Setup Counters
        # We monitor system-wide on this core (pid=-1, cpu=core_id)
        # We assume you have root/CAP_PERFMON
        self._add_counter('cycles', PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES)
        self._add_counter('instr', PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS)
        self._add_counter('llc_miss', PERF_TYPE_HW_CACHE, LLC_READ_MISS_CONFIG)

    def _add_counter(self, name, type_id, config_id):
        attr = PerfEventAttr()
        attr.type = type_id
        attr.config = config_id
        attr.disabled = 0
        attr.exclude_kernel = 1 # Ignore kernel noise, focus on MiniMD/MPI
        
        fd = perf_event_open(attr, -1, self.core_id, -1, 0)
        if fd < 0:
            err = ctypes.get_errno()
            raise OSError(f"Core {self.core_id}: perf_event_open failed. Errno: {err}. Are you root?")
        
        self.fds[name] = fd
        self.prev[name] = 0

    def get_deltas(self):
        deltas = {}
        for name, fd in self.fds.items():
            # Direct read of 8 bytes (uint64) from fd
            # This is extremely fast (microseconds)
            try:
                raw = os.read(fd, 8)
                val = struct.unpack('Q', raw)[0]
                deltas[name] = val - self.prev[name]
                self.prev[name] = val
            except OSError:
                deltas[name] = 0
        return deltas

    def set_frequency(self, target_freq):
        # I/O OPTIMIZATION:
        # Only write to the file if the value actually changes.
        # Sysfs writes are blocking and slow.
        if self.current_freq != target_freq:
            try:
                with open(self.scaling_path, 'w') as f:
                    f.write(str(target_freq))
                self.current_freq = target_freq
            except IOError:
                pass # Handle permission/path errors gracefully

    def cleanup(self):
        for fd in self.fds.values():
            os.close(fd)

def main():
    # Initialize Controllers for your specific cores
    controllers = [CoreController(c) for c in MY_CORES]
    
    print(f"Monitoring Cores: {MY_CORES}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            # Sampling Rate: 50ms (Very fast reaction)
            time.sleep(0.05) 
            
            for cc in controllers:
                d = cc.get_deltas()
                
                cyc = d['cycles']
                ins = d['instr']
                miss = d['llc_miss']

                # Avoid division by zero
                if cyc == 0: 
                    cc.set_frequency(FREQ_IDLE)
                    continue

                ipc = ins / cyc
                
                # Metric: Misses per Instruction (Risk of Memory bound)
                mpi = miss / ins if ins > 0 else 0

                # --- DECISION LOGIC (HEURISTICS) ---
                
                # STATE 1: IDLE / BLOCKING
                # If very few instructions are retiring, the core is waiting on I/O or mutex.
                # MiniMD is likely blocked.
                if ins < 10000: 
                    target = FREQ_IDLE

                # STATE 2: MPI SPINNING (The "Busy Wait" Trap)
                # OpenMPI spins on the CPU waiting for packets. 
                # Characteristics: Extremely high IPC (>2.0), Zero memory misses (L1 loop).
                # We don't want to boost frequency for this; it's waste.
                elif ipc > 1.5 and mpi < 0.001:
                    target = FREQ_IDLE

                # STATE 3: MEMORY BOUND
                # CPU is stalling waiting for RAM. Faster Hz won't help.
                # Characteristics: Low IPC, High L3 Misses.
                elif mpi > 0.01: # Threshold: 1% of instructions miss L3
                    target = FREQ_MEM

                # STATE 4: COMPUTE BOUND (MiniMD Force Calculation)
                # CPU is crunching numbers efficiently.
                # Characteristics: Moderate/High IPC, Moderate Cache usage.
                # Race to sleep: Run fast to finish fast.
                else:
                    target = FREQ_RACE

                # Apply
                cc.set_frequency(target)

    except KeyboardInterrupt:
        print("\nStopping...")
        for cc in controllers:
            cc.cleanup()

if __name__ == "__main__":
    main()
