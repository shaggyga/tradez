# Restore

Pinned Python3.12.10 environment: `python -I -B stage_c_alignment_integrity_v2/chronological_layer_checkpoint_v2.py restore --package <vault>/CHRONOLOGICAL_LAYER_20260924_082000/checkpoint/CHRONOLOGICAL_LAYER_CHECKPOINT.zip --sha256 0907ab4694fc4a03ea5f94d78a5f0b4f962f4aae69fea5c0524d3510a595b741 --destination <new-empty-directory> --run-tests`. Authenticated selected inputs and16saved base fit pairs included.221outputs/10tests reproduce;0base refits,768new chronological layer regressions intentionally recomputed,7frozen snapshots reused. Ordinary reuse reads completed outputs. Original limits include2GiB sampledRSS/output,8GiBfree disk and2100seconds restore. Same-machine relocation only.

Rollback inspection: `git worktree add --detach <new-directory> 9621eec691c0f20e77c2b0c4370290b61744a7ea`. Preserve current pointers and sealed packets.
