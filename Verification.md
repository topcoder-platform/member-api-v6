
## JWT

To make it easier to review, I create a script at `src/scripts/member-jwt.js` to generate JWT.

You need to make sure:
- `secret` equals to config `AUTH_SECRET`
- `iss` is in config `VALID_ISSUERS`
- `exp` is a timestamp is after current time so this token is not expired

After that, you can change the payload by yourself.

For example, you want to udpate member profile as user "Ghostar", you need to
- query Ghostar's user id in db
- update jwt payload userId field
- generate new jwt with this script

Sometimes the application will check `authUser.userId === member.userId`.

## Member Endpoints

You can setup seed data to test these features. Here are some useful tips.

### Update Member Email

In this endpoint, user may update email.

If user updates email, following things will happen:
- member table `newEmail`, `emailVerifyToken`, `emailVerifyTokenDate` `newEmailVerifyToken`, `newEmailVerifyTokenDate` will be updated
  - tokens are new uuid and used in verifyEmail endpoint.
- Use GET /members/<handle>/verify?token=<token> to verify token.
  - user need to verify both email and new email
  - after both emails are verified, user's email value will be updated to new email

### Member Photo Upload

There are a lot of S3 operations in this endpoint that are not updated.

To make it all working, it will take you much time and efforts.

To test this endpoint, you can comment them all and directly set photoUrl to db.

That is the only thing I update in this challenge.

## Member Trait Endpoints

All endpoints are simple CRUD. You should be able to check them by yourself.

Just be careful about the schemas used for different kind of trait.


## Prisma Schema Updates

There are some changes to prisma schema.

- Add memberTraits.hobby
- Update memberSkill.displayMode to optional
- Remove displayMode.memberSkills @ignore
- Add stats fields as discussed in forum

## Stats Migration Validation

### 2a. Environment setup

- Required env vars:
  - `DATABASE_URL` for the member database
  - `CHALLENGES_DB_URL` or `CHALLENGE_DB_URL` for the challenges database
  - `REVIEW_DB_URL` for the review database; `recalculateMemberStats.js` now reads review-api `challengeResult` rows during aggregate backfill and history supplementation, not just during rerates
- Challenge catalog prerequisite:
  - Deploy the `challenge-api-v6` migration that seeds hidden legacy `ChallengeType` rows for historical subtracks like `ARCHITECTURE`, `ASSEMBLY_COMPETITION`, and `SRM`
- Optional toggle:
  - `STATS_READ_SOURCE=legacy` keeps reads on legacy stats tables during backfill and parity validation
  - `STATS_READ_SOURCE=unified` switches reads to the new unified stats tables

### 2b. Script validation

Run `node src/scripts/verifyStatsMigration.js --samples 50` and review the output fields. Values above `50` are clamped by the script, so rerun `--samples 50` for additional random-user coverage:

- Table availability map: confirms whether each legacy and unified table needed for comparison is reachable
- Row counts per table: confirms expected population levels before cutover
- Sampled violation breakdown:
  - `track-summary`
  - `rating-mismatch`
  - `history-count`
  - `history-order`
  - `rank-completeness`
- `mostRecent` violation groups: reports `(trackId, typeId)` groups where the `memberStatsHistory` data does not have exactly one `mostRecent: true` row
- Legacy subtype resolution: the script should no longer emit `Skipping legacy ... subTrack ...` warnings for challenge types that now exist in `challenge-api-v6` as hidden legacy records

Use the violation categories to guide investigation:

- `track-summary`: compare legacy and unified aggregate counts for the sampled member, track, and type
- `rating-mismatch`: compare legacy rating values against `memberStats` and the corresponding `memberStatsHistory` most-recent row
- `history-count`: compare the number of legacy history rows to the migrated `memberStatsHistory` rows
- `history-order`: inspect chronological ordering and verify the history is sorted correctly
- `rank-completeness`: verify rated members have rank fields populated in unified stats
- `mostRecent` violation groups: inspect grouped history rows for duplicate or missing `mostRecent: true` flags

### 2c. API validation - stats read path

- `GET /members/:handle/stats`
  - Verify `develop`, `design`, and `dataScience` sub-objects are present
  - Verify `rating`, `maxRating`, `volatility`, and `rank` fields are non-null for rated members
- `GET /members/:handle/stats/history`
  - Verify the `history` array is ordered by `date` descending
  - Verify exactly one entry per `(trackId, typeId)` has `mostRecent: true`
- `GET /members/stats/distribution`
  - Verify the response shape is unchanged

### 2d. API validation - stats write path (admin/M2M)

- `POST /members/:handle/stats/refresh`
  - Requires `refresh:member_stats` scope or admin JWT
  - Body: `{ "challengeId": "<uuid>" }` (optional)
  - Verify a 200 response with a summary object
- `POST /members/:handle/stats/rerate`
  - Requires `rerate:member_stats` scope or admin JWT
  - Body: `{ "challengeId": "<uuid>", "trackId": "DEVELOP", "typeId": "Challenge" }`
  - `challengeId` is required; use the earliest applicable challenge when you need a full-history rerate for a track/type
  - Use `DEVELOP` or `DATA_SCIENCE` for `trackId`, and `Challenge` or `MARATHON_MATCH` for `typeId`; these are enum names, not DB UUIDs
  - Verify a 200 response
  - Verify `memberStatsHistory` rows are updated for the member

### 2e. Ratings validation

- For a known Development track rated member:
  - Verify `rating` in `memberStats` matches the expected value
  - Verify `memberStatsHistory` contains a row with `mostRecent: true` and matching `newRating`
- For a known Marathon Match rated member:
  - Verify `rating` in `memberStats` matches the expected value for the DATA_SCIENCE / MARATHON_MATCH track
  - Verify `memberStatsHistory` contains a row with `mostRecent: true` and matching `newRating`
- For a member with unrated challenges:
  - Verify those challenges are absent from `memberStatsHistory` rating rows

### 2f. Reports validation

- Run a known report from `reports-api-v6` that reads member stats and verify rating fields are non-null
- Compare the output against a legacy baseline captured before cutover
