# Stats Migration Cutover

## Migration Summary

| Phase | What changed | Status |
|---|---|---|
| Phase 1 | Public stats API contract frozen; `/stats/refresh` and `/stats/rerate` routes added | ✅ Done |
| Phase 2 | `recalculateMemberStats.js` backfill; `memberStats` + `memberStatsHistory` populated | ✅ Done |
| Phase 3 | TypeScript Development-track ratings engine ported and integrated | ✅ Done |
| Phase 4 | Marathon Match ratings engine with relative-scoring support | ✅ Done |
| Phase 5 | `autopilot-v6` calls `/stats/refresh` and `/stats/rerate` at challenge end | ✅ Done |
| Phase 6 | `reports-api-v6` SQL migrated to unified tables | ✅ Done |
| Phase 7 | Parity validation and consumer cutover (this phase) | 🔄 In progress |

## New M2M Scopes

| Scope | Route | Purpose |
|---|---|---|
| `refresh:member_stats` | `POST /members/:handle/stats/refresh` | Recompute aggregate stats from challenge results |
| `rerate:member_stats` | `POST /members/:handle/stats/rerate` | Re-run ratings from a given challenge forward |

## Configuration Flag

`STATS_READ_SOURCE` controls which stats tables back the read path.

- `unified` is the default and reads from `memberStats` plus `memberStatsHistory`.
- `legacy` falls back to the pre-migration table set during staged rollout or rollback validation.

Recommended rollout:

1. Backfill unified tables.
2. Run parity checks while reads stay on `legacy`.
3. Switch reads to `unified` after parity is clean and downstream consumers are verified.
4. Remove the flag only after the rollback window closes and legacy tables are no longer needed operationally.

## Operational Runbook

- Full backfill: `node src/scripts/recalculateMemberStats.js`
- Parity check: `node src/scripts/verifyStatsMigration.js --samples 50`
- Re-rate a member from a specific challenge: `POST /members/{handle}/stats/rerate` with `{ "challengeId": "...", "trackId": "...", "typeId": "..." }`
- Trigger stats refresh after a challenge: `POST /members/{handle}/stats/refresh` with `{ "challengeId": "..." }`
- Expected parity thresholds: zero rating/rank mismatches; zero history-order violations; zero `mostRecent` violation groups.
