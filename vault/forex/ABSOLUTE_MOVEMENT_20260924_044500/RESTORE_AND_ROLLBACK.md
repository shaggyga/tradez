# Restore

Use pinned Python3.12.10 environment. Run `python -I -B stage_c_alignment_integrity_v2/absolute_movement_checkpoint_v2.py restore --package <vault>/ABSOLUTE_MOVEMENT_20260924_044500/checkpoint/forex_absolute_movement.zip --sha256 a4ef4a445ceebc543dabe0a96f554ed6ec77ae4c0b8881657f55374df8e91d2a --destination <new-empty-directory> --run-tests`. The capsule contains original prepared inputs and14saved new fit pairs.72scientific payloads reproduce with0historical refits;20relocated synthetic tests pass. Inference/resource timings are separately measured. No companion archive or credentials required.

Rollback inspection: `git worktree add --detach <new-directory> f7c2c2023ad81d3bf73b56ad3beff6b70ccf47c2`. Do not overwrite live pointers with historical baseline snapshots.
