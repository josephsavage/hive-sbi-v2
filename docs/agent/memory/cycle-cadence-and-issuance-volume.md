---
name: cycle-cadence-and-issuance-volume
description: The HSBI cycle trigger (144 min or VP above about 98%), the high-VP volume regime, and the measured issuance peaks
type: project
---

A pipeline cycle starts when `max_mana_pct > mana_threshold * mana_pct_target`, or when more than `share_cycle_min` minutes (144) have passed since `last_cycle`. The cycle interval is therefore the shorter of 144 minutes and the time the fleet takes to reach the VP threshold (about 98%). The lead developer (josephsavage) stated this on 2026-07-28.

- **Low-VP regime:** one cycle every 144 minutes (2.4 h), about 120 issuances per cycle.
- **High-VP regime:** one cycle about every 15 minutes. Each cycle distributes 15 minutes of curation rewards instead of 144, so the amounts are smaller, and many small holders fall below `TOKEN_PRECISION` (0.001) and get no issuance. Measured on 2026-07-14: 1,597 issuances in 5 hours (peak hour 370), against about 250 in the low-VP regime.
- The high-VP regime is normal, not an anomaly. It recurs whenever VP stays high.
- `hivesbi/issue.py` records a measured peak of about 766 issuances per 144-minute cycle, which costs about 12.8 minutes of broadcast pacing per cycle.

**Why:** every limit that scales with cycles or issuances must fit the high-VP peak, not the average. A job inside the token cycle gate can run about every 15 minutes, not every 144.

**How to apply:** size runaway guards and scan windows against the high-VP peak. Guard any expensive step in the token cycle with its own time check, as `refresh_lassecash_stakers` does with `share_cycle_min`. Until PR #140 (merged 2026-08-22), this fact also sized the bulk chain-scan windows (`CHAIN_SCAN_LOOKBACK`, `HISTORY_SCAN_LIMIT`). PR #140 removed that scan, so those constants no longer exist. See [[token-issuance-design-invariants]].
