#!/usr/bin/env python3
import os
import time
import mmap
import ctypes
import argparse
import curses
from collections import Counter, deque

PHASE_COMPUTE      = 0
PHASE_COMMUNICATE  = 1
PHASE_EXCHANGE     = 2
PHASE_BORDERS      = 3
PHASE_REVERSE      = 4
PHASE_IO           = 5
PHASE_COMMUNICATION_ACTIVE = 6
PHASE_COMMUNICATION_PASSIVE   = 7
PHASE_DONE         = 8

PHASE_NAMES = {
    0: "COMPUTE",
    1: "COMMUNICATE",
    2: "EXCHANGE",
    3: "BORDERS",
    4: "REVERSE",
    5: "IO",
    6: "COMM_ACTIVE",
    7: "COMM_PASSIVE",
    8: "DONE",
}

PHASE_MAGIC = 0x50485331
MAX_PHASE_SLOTS = 64
RESERVED_CORE = 31
MONITOR_CORE = 30

def get_max_mhz():
    try:
        with open("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq", "r") as f:
            return float(f.read().strip()) / 1000.0
    except Exception:
        return 3600.0

MAX_MHZ = get_max_mhz()

SPARKS = " ▂▃▄▅▆▇█"

class PhaseSlot(ctypes.Structure):
    _fields_ = [
        ("seq", ctypes.c_uint32),
        ("rank", ctypes.c_int32),
        ("core", ctypes.c_int32),
        ("phase", ctypes.c_uint32),
        ("t_ns", ctypes.c_uint64),
    ]

class PhaseTable(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_uint32),
        ("nslots", ctypes.c_uint32),
        ("slots", PhaseSlot * MAX_PHASE_SLOTS),
    ]


def read_text(path):
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except Exception:
        return None


def read_freqs(num_cores):
    freqs = []
    govs = []
    for core in range(num_cores):
        fpath = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_cur_freq"
        gpath = f"/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_governor"
        raw = read_text(fpath)
        gov = read_text(gpath) or "?"
        if raw is None:
            freqs.append(None)
        else:
            try:
                freqs.append(int(raw) / 1000.0)
            except ValueError:
                freqs.append(None)
        govs.append(gov)
    return freqs, govs


def open_phase_map(path):
    if not os.path.exists(path):
        return None, None
    try:
        size = ctypes.sizeof(PhaseTable)
        f = open(path, "rb")
        mm = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
        return f, mm
    except Exception:
        return None, None


def read_phase_table(mm):
    if mm is None:
        return []
    try:
        table = PhaseTable.from_buffer_copy(mm[:ctypes.sizeof(PhaseTable)])
        if table.magic != PHASE_MAGIC or table.nslots <= 0:
            return []
        out = []
        for i in range(min(table.nslots, MAX_PHASE_SLOTS)):
            s = table.slots[i]
            if s.core >= 0:
                out.append((s.rank, s.core, s.phase, s.t_ns))
        return out
    except Exception:
        return []


def mhz_color(mhz):
    if mhz is None:
        return 5
    if mhz < 1400:
        return 2
    if mhz < 2200:
        return 3
    return 4


def governor_short(g):
    if g == "performance":
        return "P"
    if g == "userspace":
        return "U"
    return g[:1].upper() if g else "?"


def bar(value, width, max_value=MAX_MHZ):
    if value is None:
        return " " * width
    blocks = [" ", "▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"]
    ratio = max(0.0, min(1.0, value / max_value))
    total_blocks = ratio * width
    full_blocks = int(total_blocks)
    fraction = total_blocks - full_blocks
    out = "█" * full_blocks
    if full_blocks < width:
        out += blocks[int(fraction * 8)]
        out += " " * (width - full_blocks - 1)
    return out


def sparkline(values, width):
    vals = list(values)[-width:]
    if not vals:
        return ""
    low = min(vals)
    high = max(vals)
    if high - low < 1e-6:
        return SPARKS[len(SPARKS) // 2] * len(vals)
    out = []
    for v in vals:
        idx = int((v - low) / (high - low) * (len(SPARKS) - 1))
        out.append(SPARKS[idx])
    return "".join(out)


def draw_box(stdscr, y, x, h, w, title=None, color=1, title_color=6, bold_title=False):
    try:
        stdscr.attron(curses.color_pair(color))
        stdscr.addstr(y, x, "╭" + "─" * (w - 2) + "╮")
        for row in range(1, h - 1):
            stdscr.addstr(y + row, x, "│")
            stdscr.addstr(y + row, x + w - 1, "│")
        stdscr.addstr(y + h - 1, x, "╰" + "─" * (w - 2) + "╯")
        if title:
            label = f" {title} "
            if len(label) < w - 2:
                attr = curses.A_BOLD if bold_title else curses.A_NORMAL
                stdscr.addstr(y, x + 2, label, curses.color_pair(title_color) | attr)
        stdscr.attroff(curses.color_pair(color))
    except curses.error:
        pass


def write(stdscr, y, x, text, color=0, attr=0):
    try:
        H, W = stdscr.getmaxyx()
        if y < 0 or y >= H or x < 0 or x >= W:
            return
        
        # Clip text strictly to prevent curses from line-wrapping to x=0 on the next row
        max_len = W - x - 1
        if max_len <= 0:
            return
        if len(text) > max_len:
            text = text[:max_len]
            
        stdscr.addstr(y, x, text, curses.color_pair(color) | attr)
    except curses.error:
        pass


def phase_summary(phase_rows):
    c = Counter(PHASE_NAMES.get(p, str(p)) for _, _, p, _ in phase_rows)
    order = [
        "COMPUTE", "COMMUNICATE", "EXCHANGE", "BORDERS",
        "REVERSE", "IO", "COMM_ACTIVE", "COMM_PASSIVE", "DONE"
    ]
    return [(name, c[name]) for name in order if c[name] > 0]


def main_loop(stdscr, args):
    curses.curs_set(0)
    curses.start_color()
    curses.use_default_colors()
    
    # 1: Cyan (Borders), 2: Red, 3: Yellow, 4: Green, 5: White, 6: Magenta
    curses.init_pair(1, curses.COLOR_CYAN, -1)
    curses.init_pair(2, curses.COLOR_RED, -1)
    curses.init_pair(3, curses.COLOR_YELLOW, -1)
    curses.init_pair(4, curses.COLOR_GREEN, -1)
    curses.init_pair(5, curses.COLOR_WHITE, -1)
    curses.init_pair(6, curses.COLOR_MAGENTA, -1)

    history = deque(maxlen=120)
    phase_file = None
    phase_map = None
    last_reopen = 0.0

    while True:
        now = time.time()
        if phase_map is None and now - last_reopen > 1.0:
            phase_file, phase_map = open_phase_map(args.hint_file)
            last_reopen = now

        freqs, govs = read_freqs(args.cores)
        valid = [f for f in freqs if f is not None]
        avg_mhz = sum(valid) / len(valid) if valid else 0.0
        min_mhz = min(valid) if valid else 0.0
        max_mhz = max(valid) if valid else 0.0
        throttled = sum(1 for f in valid if f < args.low_mark)
        perf_count = sum(1 for g in govs if g == "performance")
        user_count = sum(1 for g in govs if g == "userspace")
        history.append(avg_mhz)

        try:
            phase_rows = read_phase_table(phase_map)
        except ValueError:
            phase_rows = []
            phase_file, phase_map = None, None

        stdscr.erase()
        H, W = stdscr.getmaxyx()

        # Header 
        title = " miniMD PHASE-AWARE DVFS DASHBOARD "
        write(stdscr, 0, max(0, (W - len(title)) // 2), title, 6, curses.A_BOLD)
        
        # Determine majority phase for the KPI header
        summary = phase_summary(phase_rows) if phase_rows else []
        dominant_phase = summary[0][0] if summary else "N/A"
        phase_col = 4 if dominant_phase in ("COMPUTE", "COMM_ACTIVE", "DONE") else 3

        # KPI row layout (Moved up to save space)
        kpis = [
            ("AVERAGE FREQ", f"{avg_mhz:6.0f} MHz", 4),
            ("MIN / MAX FREQ", f"{min_mhz:4.0f} / {max_mhz:4.0f}", 4),
            ("LOW FREQ CORES", f"{throttled:2d}", 4 if throttled == 0 else 3),
            ("PERF / USER", f"{perf_count:2d} / {user_count:2d}", 4),
            ("OVERALL PHASE", dominant_phase, phase_col),
        ]
        
        box_w = max(16, (W - len(kpis)) // len(kpis))
        total_kpi_w = box_w * len(kpis) + (len(kpis) - 1)
        start_x = max(0, (W - total_kpi_w) // 2)
        y0 = 1 # Start immediately under header
        
        for i, (k, v, v_col) in enumerate(kpis):
            x0 = start_x + i * (box_w + 1)
            if x0 + box_w >= W:
                break
            draw_box(stdscr, y0, x0, 4, box_w, color=1)
            write(stdscr, y0 + 1, x0 + max(1, (box_w - len(k)) // 2), k, 1, curses.A_BOLD)
            write(stdscr, y0 + 2, x0 + max(1, (box_w - len(v)) // 2), v, v_col, curses.A_BOLD)

        # History panel (Squished to 3 lines)
        hist_y = 5
        hist_h = 3
        draw_box(stdscr, hist_y, start_x, hist_h, total_kpi_w, "Frequency Trend", color=1, title_color=6, bold_title=True)
        hist = sparkline(history, max(10, total_kpi_w - 24))
        write(stdscr, hist_y + 1, start_x + 2, hist, 4)
        write(stdscr, hist_y + 1, start_x + total_kpi_w - 18, f"{avg_mhz:6.0f} MHz", 4, curses.A_BOLD)

        # Left: core grid
        grid_y = 8
        left_w = max(50, int(W * 0.65))
        right_x = left_w + 1
        right_w = W - right_x
        grid_h = H - grid_y - 2
        draw_box(stdscr, grid_y, 0, max(8, grid_h), left_w, "Per-Core Frequencies", color=1, title_color=6, bold_title=True)

        cols = 2 if left_w < 90 else 3
        rows_per_col = (args.cores + cols - 1) // cols
        inner_w = left_w - 4
        col_w = max(24, inner_w // cols)
        bar_w = max(8, col_w - 19)

        for idx in range(args.cores):
            c = idx // rows_per_col
            r = idx % rows_per_col
            y = grid_y + 2 + r
            x = 2 + c * col_w
            if y >= H - 2 or x + col_w >= left_w:
                continue
            
            mhz = freqs[idx]
            gov = govs[idx]
            color = mhz_color(mhz)
            
            if mhz is None:
                write(stdscr, y, x, f"cpu{idx:02d}", 5)
                write(stdscr, y, x + 6, "[N/A]", 5)
            else:
                gov_str = governor_short(gov)
                gov_col = 4 if gov_str == "P" else 3 if gov_str == "U" else 5
                
                write(stdscr, y, x, f"cpu{idx:02d}", 5)
                write(stdscr, y, x + 6, f"[{gov_str}]", gov_col, curses.A_BOLD)
                write(stdscr, y, x + 10, f"{mhz:4.0f}", color, curses.A_BOLD)
                write(stdscr, y, x + 15, "MHz", 1)
                write(stdscr, y, x + 19, bar(mhz, bar_w), color)

        # Right: phases and legend
        draw_box(stdscr, grid_y, right_x, max(8, grid_h), right_w, "Application Phases", color=1, title_color=6, bold_title=True)
        py = grid_y + 2
        
        if not phase_rows:
            # Render a nice blinking alert box if it's waiting
            msg = f"WAITING FOR HINT FILE"
            alert_w = len(msg) + 6
            alert_x = right_x + max(1, (right_w - alert_w) // 2)
            alert_y = grid_y + max(2, grid_h // 2 - 2)
            
            draw_box(stdscr, alert_y, alert_x, 5, alert_w, "STATUS", color=3, title_color=3, bold_title=True)
            write(stdscr, alert_y + 2, alert_x + 3, msg, 3, curses.A_BLINK | curses.A_BOLD)
        else:
            # summary already calculated above
            agg_str = " | ".join(f"{name}: {count}" for name, count in summary[:3])
            write(stdscr, py - 1, right_x + 2, f"► AGGREGATES: {agg_str}", 4, curses.A_BOLD)
            
            write(stdscr, py, right_x + 2, "► CORE MAPPING DETAILS", 6, curses.A_BOLD)
            
            phase_rows_sorted = sorted(phase_rows, key=lambda x: x[0])
            start_py = py + 1
            max_rows = grid_h - (start_py - grid_y) - 1
            
            for i, (rank, core, phase, _) in enumerate(phase_rows_sorted):
                if i >= max_rows:
                    break
                    
                cur_y = start_py + i
                cur_x = right_x + 2
                
                pname = PHASE_NAMES.get(phase, str(phase))
                col = 4 if pname in ("COMPUTE", "COMM_ACTIVE") else 2 if pname in ("IO", "COMM_PASSIVE") else 3
                
                # Render vertically down a single unbroken column
                write(stdscr, cur_y, cur_x + 1, f"Rank {rank:02d}", 5)
                write(stdscr, cur_y, cur_x + 9, f"→ cpu{core:02d}", 1)
                write(stdscr, cur_y, cur_x + 18, f"{pname}", col, curses.A_BOLD)
                    
            py = start_py + min(len(phase_rows_sorted), max_rows)

        footer = f" TARGET: {os.path.basename(args.hint_file)} | refresh: {args.refresh:.2f}s | Q:QUIT  R:REFRESH "
        write(stdscr, H - 1, max(0, (W - len(footer)) // 2), footer[:max(0, W)], 1)

        stdscr.refresh()
        stdscr.timeout(int(args.refresh * 1000))
        ch = stdscr.getch()
        if ch in (ord('q'), ord('Q')):
            break
        if ch in (ord('r'), ord('R')):
            try:
                if phase_map is not None:
                    phase_map.close()
                if phase_file is not None:
                    phase_file.close()
            except Exception:
                pass
            phase_file, phase_map = None, None


def main():
    ap = argparse.ArgumentParser(description="Live terminal dashboard for miniMD DVFS demos")
    ap.add_argument("--cores", type=int, default=32, help="Number of CPU cores to display")
    ap.add_argument("--refresh", type=float, default=0.5, help="Refresh period in seconds")
    ap.add_argument("--low-mark", type=float, default=1600.0, help="Count frequencies below this as throttled")
    ap.add_argument("--hint-file", default=os.environ.get("PHASE_HINT_PATH", "/dev/shm/minimd_phase_hints.bin"), help="Shared-memory phase hint file")
    args = ap.parse_args()
    curses.wrapper(main_loop, args)


if __name__ == "__main__":
    main()
