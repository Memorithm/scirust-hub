# FLAT SHA ledger (V0-4)

Canon repository: `Memorithm/FLAT-ATTENTION` default branch `main`.

| Consumer | Pin observed 2026-09-12 | Action |
|---|---|---|
| FLAT-ATTENTION HEAD | `4529a2079434965e13e90ddd2e98ecc88ee0cb3a` | canon tip |
| NoiseLab | `4529a2079434965e13e90ddd2e98ecc88ee0cb3a` | already aligned |
| SciRust `flat-attention-planner` / `flat-elastic-kernel` | `75d3bd684643aedb98f55a892f93d727a8187cea` | drift — do not bump until `cargo check --features flat-autotune` is green |

Rule: one SHA in flight. The bump lives in SciRust, not in a third pin.
