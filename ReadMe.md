# Topcoder Member API v6

## Technology Stack

- Node.js 26.5.0 (use the version pinned in `.nvmrc`)
- TypeScript and NestJS 11, with the established Express routes mounted through Nest
- Prisma ORM 7 with the PostgreSQL driver adapter
- pnpm 11.15.1
- PostgreSQL
- Docker and Docker Compose

The service does not consume Kafka messages. It continues to publish events through
the Bus API wrapper, so the existing Bus API and Auth0 configuration remains unchanged.

## Install, Build, and Run

```bash
nvm use
pnpm install
pnpm lint
pnpm build
pnpm start
```

For development with TypeScript watch mode, use `pnpm start:dev`. Production runs the
compiled NestJS entrypoint at `dist/main.js`.

## Database Setup

Please setup PostgreSQL first. If you are using docker, you can run:
```bash
docker run -d --name memberdb -p 5432:5432 \
  -e POSTGRES_USER=johndoe -e POSTGRES_DB=memberdb \
  -e POSTGRES_PASSWORD=mypassword \
  postgres:16
```

After that, please set db URL environment variables:
```bash
export DATABASE_URL="postgresql://johndoe:mypassword@localhost:5432/memberdb"
export SKILLS_DB_URL="postgresql://johndoe:mypassword@localhost:5432/skillsdb"
export CHALLENGE_DB_URL="postgresql://johndoe:mypassword@localhost:5432/challengedb?schema=challenges"
```

These variables are important since they're required by Prisma clients.
`CHALLENGE_DB_URL` should target the same PostgreSQL instance/database used by `challenge-api-v6`.

If you want to do anything with database, these variables are necessary.

## Database Scripts

Before running db scripts, please make sure you have setup db and config db url as above.

```bash
# set db url values
export DATABASE_URL="postgresql://johndoe:mypassword@localhost:5432/memberdb"
export SKILLS_DB_URL="postgresql://johndoe:mypassword@localhost:5432/skillsdb"
export CHALLENGE_DB_URL="postgresql://johndoe:mypassword@localhost:5432/challengedb?schema=challenges"

# install dependencies and generate Prisma clients
pnpm install
```

Here are some helpful scripts for db operations.

```bash
# create db tables
pnpm init-db

# Clear all db data
pnpm clear-db

# create test data
pnpm seed-data

# Reset db
pnpm reset-db
```

## Database Seed Data

I have created a script to download data from dev environment and a script to load them into db.

To use them, you should:
- Make sure you have started db.
- Check configs in `src/scripts/config.ts`. Add some handle if you like.
- Open a terminal and navigate to codebase folder. Set `DATABASE_URL` and `SKILLS_DB_URL` above.
- Run `pnpm install`.
- Use `pnpm download-data` to download profile data.
- Run `pnpm clear-db` to clear db data first.
- Run `pnpm seed-data` to import data into db.


## Configuration

Configuration for the application is at `config/default.js`.
The following parameters can be set in config files or in env variables:

- LOG_LEVEL: the log level, default is 'debug'
- PORT: the server port, default is 3000
- AUTH_SECRET: The authorization secret used during token verification.
- VALID_ISSUERS: The valid issuer of tokens.
- IDENTITY_DB_URL: PostgreSQL connection string for Identity schema reads.
- CHALLENGE_DB_URL: PostgreSQL connection string for challenge schema reads (format: `postgresql://user:password@host:port/database?schema=challenges`, and should point to the same database used by challenge-api-v6).
- VANILLA_DB_URL: MySQL connection string for the Vanilla forums database.
- AUTH0_URL: AUTH0 URL, used to get M2M token
- AUTH0_PROXY_SERVER_URL: AUTH0 proxy server URL, used to get M2M token
- AUTH0_AUDIENCE: AUTH0 audience, used to get M2M token
- TOKEN_CACHE_TIME: AUTH0 token cache time, used to get M2M token
- AUTH0_CLIENT_ID: AUTH0 client id, used to get M2M token
- AUTH0_CLIENT_SECRET: AUTH0 client secret, used to get M2M token
- BUSAPI_URL: Bus API URL
- KAFKA_ERROR_TOPIC: Kafka error topic used by bus API wrapper
- GROUPS_API_URL: Groups API URL
- SENDGRID_API_KEY: SendGrid API key used for member email activity lookups.
- AMAZON.AWS_ACCESS_KEY_ID: The Amazon certificate key to use when connecting.
- AMAZON.AWS_SECRET_ACCESS_KEY: The Amazon certificate access key to use when connecting.
- AMAZON.AWS.SESSION_TOKEN: The user session token, used when developing locally against the TC dev AWS services
- AMAZON.AWS_REGION: The Amazon certificate region to use when connecting.
- AMAZON.PHOTO_S3_BUCKET: the AWS S3 bucket to store photos, optionally followed by an object key prefix (for example, `bucket/member/profile`)
- AMAZON.S3_API_VERSION: the AWS S3 API version
- FILE_UPLOAD_SIZE_LIMIT: the file upload size limit in bytes
- PHOTO_URL_TEMPLATE: photo URL template, its <key> will be replaced with the generated file name
- VERIFY_TOKEN_EXPIRATION: verify token expiration in minutes
- EMAIL_VERIFY_AGREE_URL: email verify agree URL, the <emailVerifyToken> will be replaced with generated verify token
- EMAIL_VERIFY_DISAGREE_URL: email verify disagree URL
- SCOPES: the configurable M2M token scopes, refer `config/default.js` for more details
- MEMBER_SECURE_FIELDS: Member profile identifiable info fields, only admin, M2M, or member himself can fetch these fields
- COMMUNICATION_SECURE_FIELDS: Member contact information, accessible by admins, managers, and copilots - anyone with a role in the AUTOCOMPLETE_ROLES array
- MEMBER_TRAIT_SECURE_FIELDS: Member traits identifiable info fields, only admin, M2M, or member himself can fetch these fields
- MISC_SECURE_FIELDS: Misc identifiable info fields, only admin, M2M, or member himself can fetch these fields
- STATISTICS_SECURE_FIELDS: Member Statistics identifiable info fields, only admin, M2M, or member himself can fetch these fields
- STATS_READ_SOURCE: Controls stats read backend, `unified` (default, new tables) or `legacy` (pre-refactor tables)
- RATING_PATHS: JSON array of tag- and skill-based Development Challenge and Marathon Match rating paths. Each entry has `name`, optional `tags`, optional `skillIds`, and optional `track` (`DATA_SCIENCE`, `DEVELOP`, or `DEVELOPMENT`); at least one of `tags` or `skillIds` is required. Tags match any listed challenge tag, while skill IDs require every listed challenge skill. Defaults to `[{"name":"AI Engineering","track":"DATA_SCIENCE","tags":["AI","AI Exponential League"]}]`. Re-rate a path by passing `ratingName` to `POST /members/{handle}/stats/rerate`; rows are stored under `{track} / {name}` in unified stats.
- HEALTH_CHECK_TIMEOUT: maximum primary members database probe duration in milliseconds before the health endpoint returns 503

Set the following environment variables used by bus API to get TC M2M token (use 'set' insted of 'export' for Windows OS):
```
export AUTH0_CLIENT_ID=
export AUTH0_CLIENT_SECRET=
export AUTH0_URL=
export AUTH0_AUDIENCE=
```

Also properly configure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, PHOTO_S3_BUCKET config parameters.

Test configuration is at `config/test.js`. You don't need to change them.
The following test parameters can be set in config file or in env variables:

- ADMIN_TOKEN: admin token
- USER_TOKEN: user token
- EXPIRED_TOKEN: expired token
- INVALID_TOKEN: invalid token
- M2M_FULL_ACCESS_TOKEN: M2M full access token
- M2M_READ_ACCESS_TOKEN: M2M read access token
- M2M_UPDATE_ACCESS_TOKEN: M2M update (including 'delete') access token
- S3_ENDPOINT: endpoint of AWS S3 API, for unit and e2e test only; default to `localhost:9000`

## Challenge Point Profile Data

Point-based challenge prizes are mirrored into member-api so profile pages can show totals and a per-challenge breakdown without reading tc-finance-api.

Autopilot replaces the rows for one challenge with:

```http
PUT /v6/members/challenge-points/{challengeId}
Content-Type: application/json

{
  "challengeName": "AI Points Challenge",
  "points": [
    { "userId": 123456, "placement": 1, "points": 500 }
  ]
}
```

`GET /v6/members/{handle}` includes a public `challengePoints` object by default:

```json
{
  "challengePoints": {
    "total": 2847,
    "challenges": 14,
    "details": [
      {
        "challengeId": "challenge-uuid",
        "challengeName": "AI Points Challenge",
        "userId": 123456,
        "placement": 1,
        "points": 500
      }
    ]
  }
}
```

## Rating Path Operations

Historical rating paths are configured with `RATING_PATHS`. The default AI path is:

```json
[
  {
    "name": "AI Engineering",
    "track": "DATA_SCIENCE",
    "tags": ["AI", "AI Exponential League"]
  }
]
```

The `track` value controls where the rating is stored in unified stats. Use `DATA_SCIENCE` to store rows under `DATA_SCIENCE / AI Engineering`, or `DEVELOP` / `DEVELOPMENT` to store rows under `DEVELOP / AI Engineering`. The rating engine includes both Marathon Match results and `challenge` type Development Challenge results when the challenges match the configured tags or skill IDs. When a configured rating name does not already exist as a `ChallengeType`, rerate creates a deterministic custom type row so unified `memberStats` foreign keys remain valid.

### Rating Calculation Details

Native Development Challenge ratings, native Marathon Match ratings, and configured rating paths all use the shared Qubits implementation in `src/ratings/qubitsAlgorithm.ts`, following Topcoder's documented rating algorithm: https://www.topcoder.com/thrive/articles/Ratings.

- Initial user state: a member without prior ratings starts the replay as `{ rating: 0, volatility: 0, numRatings: 0 }`, but the first calculation treats them as an unrated Qubits participant with baseline rating `1200` and volatility `515`. The first persisted rating is the calculated post-challenge rating, not a stored `1200` seed. First persisted volatility is forced to `385`.
- Incremental rating changes: each rated challenge is replayed in event order. Previously rated participants are updated first using only the previously rated field, then unrated participants are rated against everyone in the challenge. Each pass calculates from frozen pre-event participant states, matching the legacy Java process. Scores are converted into ranks, with higher normalized score ranked better and tied scores receiving the average tied rank. Expected rank starts at `0.5` and adds each comparison participant's win probability against the target: `0.5 * (erf((opponentRating - targetRating) / sqrt(2 * (opponentVolatility^2 + targetVolatility^2))) + 1)`. Expected and actual ranks are converted with `-normsinv((rank - 0.5) / participantCount)`. The target "performed as" value is `oldRating + challengeFactor * (actualPerformance - expectedPerformance)`. The raw new rating is `(oldRating + weight * performedAs) / (1 + weight)`, where `weight = 1 / (1 - (((0.60 - 0.18) / (timesPlayed + 1)) + 0.18)) - 1`, reduced by `10%` at ratings `>= 2000` and by `20%` at ratings `>= 2500`. The rating delta is capped to `150 + 1500 / (timesPlayed + 2)`, then floored at `1`, before rounding.
- Volatility: existing participants use their stored volatility in both win probability and challenge factor calculations. The challenge factor is `sqrt((sum(volatility^2) / participantCount) + (sum((rating - averageRating)^2) / (participantCount - 1)))` across comparison participants. After the capped rating change, new volatility is `sqrt(((newRating - oldRating)^2 / weight) + (oldVolatility^2 / (weight + 1)))`, rounded to an integer. Rating rerates store `oldVolatility` and `newVolatility` in `memberStatsHistory`; history-seeded rerates use `newVolatility` when present and fall back to the default volatility `515` only for older rows without a checkpoint.

To re-run a configured rating path for one member, call the member stats rerate endpoint with `ratingName`:

```http
POST /v6/members/{handle}/stats/rerate
Content-Type: application/json

{
  "challengeId": "earliest-tagged-challenge-id-for-that-member",
  "ratingName": "AI Engineering"
}
```

The request requires an admin token or an M2M token with `rerate:member_stats` or `all:user_profiles`. Use the earliest matching challenge the member competed in when you want complete rating history for the path. If a later challenge is supplied, prior matching events can still influence the calculated rating during replay, but persisted history starts at the supplied challenge.

At challenge completion time, callers can rerate every submitter and every applicable rating dimension with the challenge-scoped endpoint:

```http
POST /v6/members/stats/rerate-challenge
Content-Type: application/json

{
  "challengeId": "completed-challenge-id"
}
```

The endpoint discovers submitters from review results, rerates the supported native track/type rating, and also rerates any configured named paths that match the challenge tags or skills. It requires an admin token or an M2M token with `rerate:member_stats` or `all:user_profiles`.

To re-run native Marathon Match ratings for every discovered MM competitor from the beginning of their MM history, use the bulk native MM script:

```bash
pnpm rerate-marathon-matches --dry-run
pnpm rerate-marathon-matches --concurrency 5
```

The script discovers completed Marathon Match challenges by `ChallengeType` id and also merges distinct challenge IDs already present in `DATA_SCIENCE / MARATHON_MATCH` `memberStatsHistory`, so migrated legacy MM rows that predate ChallengeType classification are scanned too. Migrated numeric rows such as `MM 145` are merged with canonical Challenge API rows such as `Marathon Match 145` by MM round number before participant discovery, and rerate seeding prefers complete canonical UUID checkpoints over duplicate legacy rows. The script discovers competitors from final review summations using both canonical challenge UUIDs and legacy numeric challenge IDs, filters out user IDs that do not exist in member storage, and calls the native MM rerate engine with no starting challenge so each member is replayed from their first MM event. Current MM ranks are recalculated once after the batch rather than once per member. Historical MM replay does not require `MM_DB_URL` or the marathon-match-api schema.

After a rerate, migrated numeric history rows can still exist beside the canonical UUID rows until they are physically cleaned from `memberStatsHistory`. When the members and challenges schemas are available in the same Postgres database, preview and apply the cleanup with:

```bash
pnpm dedupe-marathon-match-history --dry-run --user-id 40562752
pnpm dedupe-marathon-match-history --apply --user-id 40562752
```

The cleanup deletes only migrated numeric `DATA_SCIENCE / MARATHON_MATCH` history rows that match a complete official Challenge API Marathon Match UUID row for the same member and MM round number. It leaves Challenge API helper challenges such as writer, tester, task, and placement award records intact because those are separate challenge records, not native Marathon Match rating history. After native MM rerates exist for a member, profile history also hides remaining orphan numeric MM rows that cannot be resolved to a Challenge API UUID or matched to a canonical MM round for that member.

To re-run a configured rating path for every member who participated in the configured challenge set, use the bulk script instead of the member-scoped API:

```bash
pnpm rerate-rating-path --rating-name "AI Engineering" --dry-run
pnpm rerate-rating-path --rating-name "AI Engineering" --concurrency 5
```

The script discovers distinct user IDs from all rated challenges matching the configured path, then rerates each user from the start of the path so complete history is written without requiring individual handles or challenge IDs. It writes successfully processed user IDs to `rerateRatingPath.processedUserIds.json` by default. Marathon Match path events are also read from review summations and do not require `MM_DB_URL`.

Useful script options:

```bash
pnpm rerate-rating-path --rating-name "AI Engineering" --limit 100
pnpm rerate-rating-path --rating-name "AI Engineering" --user-id 12345
pnpm rerate-rating-path --rating-name "AI Engineering" --user-ids 12345,67890
pnpm rerate-rating-path --rating-name "AI Engineering" --processed-user-ids-path /tmp/ai-rerated-users.json
```

To view one member's rating and history for a `DATA_SCIENCE` path:

```http
GET /v6/members/{handle}/stats?trackId=DATA_SCIENCE&typeId=AI%20Engineering
GET /v6/members/{handle}/stats/history?trackId=DATA_SCIENCE&typeId=AI%20Engineering
```

To view one member's rating and history for a `DEVELOPMENT` path, use the canonical unified track name:

```http
GET /v6/members/{handle}/stats?trackId=DEVELOP&typeId=AI%20Engineering
GET /v6/members/{handle}/stats/history?trackId=DEVELOP&typeId=AI%20Engineering
```

To view aggregate rating distribution data:

```http
GET /v6/members/stats/distribution?track=DATA_SCIENCE&subTrack=AI
GET /v6/members/stats/distribution?track=DEVELOP&subTrack=AI
```

There is no list endpoint that returns every member with a specific custom rating type. To inspect the complete current list without adding an endpoint, query `members.memberStats` directly for the resolved track ID and `typeId = 'AI'`.

## AWS S3 Setup
Go to https://console.aws.amazon.com/ and login. Choose S3 from Service folder and click `Create bucket`. Following the instruction to create S3 bucket.
After creating bucket, click `Permissions` tab of the bucket, in the `Block public access` section, disable the block, so that public access
is allowed, then we can upload public accessible photo to the bucket.

## Local Mock Service

To make local development easier, I create a mock server at `mock`.

You can start it with `node mock/mock-api.js` and it will listen to port `4000`

This mock service will simulate request and responses for other APIs like auth0 and event bus API.

## Local Configs

Please run following commands to set necessary configs:

```bash
export AUTH0_URL="http://localhost:4000/v5/auth0"
export BUSAPI_URL="http://localhost:4000/v5"
export AUTH0_CLIENT_ID=xyz
export AUTH0_CLIENT_SECRET=xyz
export USERFLOW_PRIVATE_KEY=mysecret
export GROUPS_API_URL="http://localhost:4000/v5/groups"
```

These commands will set auth0 and event bus api to local mock server.

## Local Deployment

- Make sure you have started db and set `DATABASE_URL`.
- Make sure you have create db structure. Seed data is optional.
- Install dependencies with `pnpm install`.
- Compile the application with `pnpm build`.
- Start the compiled NestJS application with `pnpm start`.
- App is running at port 3000. You can visit `http://localhost:3000/v6/members/health`.
  Startup warms the primary members database connection before listening, and
  concurrent health requests share one lightweight connectivity query.


## Tests


Make sure you have followed above steps to
- setup db and config db url
- setup local mock api and set local configs
  - it will really call service and mock api


Then you can run:
```bash
pnpm test
```

## Verification
Refer to the verification document `Verification.md`

## Migration & Cutover

The stats migration and staged-cutover runbook now lives in [`CUTOVER.md`](./CUTOVER.md).

Use `STATS_READ_SOURCE=unified` for the unified `memberStats` / `memberStatsHistory` read path and `STATS_READ_SOURCE=legacy` only as a staged rollback switch during parity validation.
