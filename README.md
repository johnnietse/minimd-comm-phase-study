# miniMD / miniFE communication-phase study (backup)

Backup of a study run on CAC Frontenac (Queen's University) between
January and March 2026, copied on 2026-11-05 from the cluster home
directory of account `hpc6081`. Private: kept off the public
`llm-d-epp-energy` repo because it is a separate project.

## Contents

| Path | What it is |
|---|---|
| `miniMD/` | Mantevo miniMD with the study's changes: instrumented communication code, monitor scripts, `ref/communication_test_results/`, and build outputs |
| `miniFE/` | Mantevo miniFE with local changes and build outputs |
| `trial_optimized.py` | "Intelligent communication phase monitor with automated optimization testing", v16.0 |
| `test_gui_web.py`, `test_gui_windowed.py` | miniMD communication-phase test runners (web and windowed GUIs) |
| `results/monitor_*.csv`, `results/profile_comparison_*.json` | Monitor traces and profile comparisons from the runs of 2026-01-21 (32 ranks, `./miniMD_openmpi`, `in.lj.miniMD`) |
| `upstream/` | Where `miniMD/` and `miniFE/` came from, and what was changed |

## Upstream code and changes

The two code folders were git clones of the public Mantevo repositories. Their
`.git` folders are not included; instead `upstream/` records:

- `miniMD.txt`: <https://github.com/Mantevo/miniMD.git> at commit
  `2662065eb2b2264a673bda780a23d296f05a2c87` (2025-03-04)
- `miniFE.txt`: <https://github.com/Mantevo/miniFE.git> at commit
  `abe328816d84afc319c482d6bc8df414b8f90d79` (2023-07-17)
- `*-changes-to-upstream-files.patch`: `git diff HEAD` of upstream files
  that were modified
- `*-git-status.txt`: every modified and new file, as git reported it

To rebuild a clone with the changes: clone the upstream repo, check out the
recorded commit, apply the patch, then copy the new files from this folder.

## Faithfulness

Every file is stored byte-for-byte (`.gitattributes` turns off line-ending
conversion). The copy was checksum-verified against the cluster with
`rsync -c`: 0 files differ. The originals also remain on the cluster in
`/global/home/hpc6081/`, which CAC backs up.

## One file that is not a copy

`miniMD/ref/chk` was a symbolic link to `/lscratch/slurm-job-3121370-1/chk`,
the node-local scratch of an old Slurm job. That target no longer existed, so
the link had no content; `miniMD/ref/chk.symlink.txt` records where it
pointed.

## File names containing ":"

Some result files on the cluster have `:` in their names (timestamps such as
`miniFE.32x32x32.P1.T1.2025-11-10-22:27:47.yaml`). Windows does not allow `:`
in file names, so the copy made through WSL stores each `:` as the
private-use character U+F03A. Contents are unchanged; only that character in
the name differs. To restore the original names, replace U+F03A with `:`.

## Upstream .gitignore

The upstream folders' `.gitignore` files exclude build outputs and run
results. In this backup all 31 such files were added anyway (`git add -f`),
so every file from the cluster is here.
