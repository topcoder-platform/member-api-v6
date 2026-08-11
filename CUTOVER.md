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

### 1. Pre-flight checklist

- Confirm all three database env vars are set: `DATABASE_URL`, `CHALLENGES_DB_URL` or `CHALLENGE_DB_URL`, `REVIEW_DB_URL`
- Confirm `REVIEW_DB_URL` points to the review-api database that contains `challengeResult`; this is separate from the member Prisma schema deployment
- Confirm the `challenge-api-v6` migration that adds hidden legacy challenge types has been deployed so legacy subtracks such as `ARCHITECTURE`, `ASSEMBLY_COMPETITION`, and `SRM` can be resolved during stats backfill
- Confirm `STATS_READ_SOURCE=legacy` is set so reads stay on legacy tables during backfill
- Confirm `member-api-v6` is running and `/members/health` returns 200

### 2. Phase A - Full backfill (run once, idempotent)

| Step | Command | Notes |
|---|---|---|
| A1 | `pnpm recalculate-member-stats` | Full backfill of all users; writes `memberStats` + `memberStatsHistory`; starts aggregate rows from legacy stats tables when present, supplements them with newer review-api `challengeResult` rows, falls back to review-api `challengeResult` or `ChallengeWinner` when legacy rows do not exist, and also supplements `memberStatsHistory` with completed review/winner-backed challenges that were never written to the legacy history tables. Preserves existing rating/rank fields, processes users with bounded parallelism (`--concurrency`, default `4`), and checkpoints processed user IDs to `./recalculateMemberStats.processedUserIds.json` after each completed batch |
| A1a | `pnpm recalculate-member-stats --skip-rerate` | Recommended for the initial full load when total runtime matters; still backfills legacy rating/rank fields but skips the expensive Development rerate replay so rerates can be run separately in Phase D |
| A2 | `pnpm recalculate-member-stats --skip-history` | Re-run aggregate stats only if history is already seeded; full-user runs replace stale public unified-only rows for migrated members while leaving legacy-backed parents intact |
| A3 | `pnpm recalculate-member-stats --user-id <userId>` | Single-user backfill for spot checks |
| A4 | `pnpm recalculate-member-stats --track-id <uuid-or-name>` | Scope backfill to one track |
| A4a | `pnpm recalculate-member-stats --concurrency 8` | Increase per-batch user parallelism when the database can tolerate higher read/write load |
| A4b | `pnpm recalculate-member-stats --processed-user-ids-path /tmp/recalculateMemberStats.processedUserIds.json` | Write the processed-user checkpoint file to a custom path for long-running validation batches |
| A5 | `pnpm recalculate-member-stats --csv-only --csv-path /tmp/stats.csv` | Dry-run CSV output without writing to DB |

- If the script exits before processing users with `REVIEW_DB_URL does not expose challengeResult`, the configured review database is missing the review-api table. Deploy `review-api-v6` migrations or update `REVIEW_DB_URL` to the correct database before retrying.
- For large environments, prefer `--skip-rerate` during the bulk backfill and run the Development rerate pass separately afterward. The rerate replay is usually the longest phase by a wide margin because it replays rated challenge history one challenge at a time.
- The script logs per-batch timing breakdowns for preload queries, aggregate generation, stats/history writes, rerates, and checkpoint writes, plus slow-user samples for the aggregate/history/rerate phases.

### 3. Phase B - Parity validation (run before switching read source)

| Step | Command | Pass threshold |
|---|---|---|
| B1 | `pnpm verify-stats-migration --samples 50` | Zero `rating-mismatch`, zero `history-order`, zero `mostRecent` violation groups |
| B2 | `pnpm verify-stats-migration --samples 50` | Repeat on a second random sample batch; same thresholds |
| B3 | `pnpm verify-stats-migration --samples 1` | Manual review of JSON output for a known high-activity member against a specific userId (edit script or use `--user-id` if added) |

- `verifyStatsMigration.ts` clamps `--samples` to a maximum of `50`; rerun the command with `--samples 50` if you want additional random-user coverage.

### 4. Phase C - Switch read source

- Set `STATS_READ_SOURCE=unified` in environment and redeploy `member-api-v6`
- Verify `GET /members/:handle/stats` returns correct data for 3-5 known members
- Verify `GET /members/:handle/stats/history` returns ordered history with correct `mostRecent` flags

### 5. Phase D - Ratings re-run (Development track)

| Step | Endpoint | Body | Notes |
|---|---|---|---|
| D1 | `POST /members/:handle/stats/rerate` | `{ "challengeId": "<earliest-dev-challenge-uuid>", "trackId": "DEVELOP", "typeId": "Challenge" }` | Use the earliest Development/Challenge `challengeId` for a full-history rerate |
| D2 | `POST /members/:handle/stats/rerate` | `{ "challengeId": "<starting-dev-challenge-uuid>", "trackId": "DEVELOP", "typeId": "Challenge" }` | Re-rates Development/Challenge data from a specific challenge forward |
| D3 | `POST /members/:handle/stats/rerate` | `{ "challengeId": "<starting-mm-challenge-uuid>", "trackId": "DATA_SCIENCE", "typeId": "MARATHON_MATCH" }` | Use the earliest Marathon Match `challengeId` for full history, or a later one for a partial rerate |
| D4 | `pnpm rerate-marathon-matches --dry-run` then `pnpm rerate-marathon-matches --concurrency 5` | n/a | Bulk rerates native Marathon Match ratings for every discovered competitor from the beginning. Challenge discovery uses only the Marathon Match `ChallengeType` id, so Data Science and Development-track MM imports are replayed together into `DATA_SCIENCE / MARATHON_MATCH`; migrated numeric rows are de-duplicated against canonical UUID rows by MM round number during replay; no `MM_DB_URL` or marathon-match-api schema is required |
| D5 | `pnpm dedupe-marathon-match-history --dry-run` then `pnpm dedupe-marathon-match-history --apply` | n/a | Physically removes migrated numeric `DATA_SCIENCE / MARATHON_MATCH` `memberStatsHistory` rows that duplicate complete official Marathon Match UUID history rows for the same user and MM round. This requires the members and challenges schemas to be accessible from the same Postgres database and leaves non-MM helper Challenge API records intact |

- Auth: Bearer token with `rerate:member_stats` scope, or admin JWT
- `challengeId` is required for rerates; use the earliest applicable challenge when you need a full-history rerate.
- `trackId` and `typeId` are API enum names, not DB UUIDs.

### 6. Phase E - Stats refresh after a challenge

| Step | Endpoint | Body | Notes |
|---|---|---|---|
| E1 | `POST /members/:handle/stats/refresh` | `{ "challengeId": "<uuid>" }` | Recomputes aggregate stats; callable by autopilot-v6 via M2M |

- Auth: Bearer token with `refresh:member_stats` scope, or admin JWT

### 7. Phase F - Reports cutover

- Confirm `reports-api-v6` SQL queries reference unified tables (`memberStats`, `memberStatsHistory`, `memberMaxRating`)
- Run a sample report for a known member and compare output against legacy report output
- Confirm `mm-stats.sql` and `recent-member-data.sql` return non-null rating values

### 8. Phase G - Rollback procedure

If parity checks fail after switching to `unified`:

1. Set `STATS_READ_SOURCE=legacy` and redeploy - reads immediately fall back to legacy tables; no data loss
2. Investigate violations reported by `verifyStatsMigration.ts` output
3. Re-run backfill for affected users: `pnpm recalculate-member-stats --user-id <userId>`
4. Re-run parity check: `pnpm verify-stats-migration --samples 50`
5. Switch back to `unified` only after zero violations

### 9. Go/No-Go evidence gates (required before removing `STATS_READ_SOURCE` flag)

- [ ] Two `verifyStatsMigration.ts --samples 50` runs report zero errors across all violation categories
- [ ] `memberStatsHistory mostRecent violation groups` = 0
- [ ] Non-null rating/rank rows in `memberStats` >= non-null rows in legacy tables (verified by script output)
- [ ] `GET /members/:handle/stats` response shape is identical to legacy for 5 sampled members
- [ ] `GET /members/:handle/stats/history` returns correct chronological order for 5 sampled members
- [ ] At least one challenge-end `POST /stats/refresh` call succeeded via autopilot-v6 M2M token
- [ ] At least one `POST /stats/rerate` call succeeded for a Development track member
- [ ] At least one `POST /stats/rerate` call succeeded for a Marathon Match member
- [ ] Reports API returns non-null ratings for known rated members
