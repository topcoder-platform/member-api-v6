# Topcoder Member API v5

## Dependencies

- nodejs https://nodejs.org/en/ (v10)
- prisma
- PostgreSQL
- Docker, Docker Compose

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
```

This variable is important since it's required by prisma.

If you want to do anything with database, this variable is necessary.

## Database Scripts

Before running db scripts, please make sure you have setup db and config db url as above.

```bash
# set db url values
export DATABASE_URL="postgresql://johndoe:mypassword@localhost:5432/memberdb"

# install dependencies
npm install
```

Here are some helpful scripts for db operations.

```bash
# create db tables
npm run init-db

# Clear all db data
npm run clear-db

# create test data
npm run seed-data

# Reset db
npm run reset-db
```

## Database Seed Data

I have created a script to download data from dev environment and a script to load them into db.

To use them, you should:
- Make sure you have started db.
- Check configs in `src/scripts/config.js`. Add some handle if you like.
- Open a terminal and navigate to codebase folder. Set `DATABASE_URL` above.
- Run `npm install`.
- Use `node src/scripts/download.js` to download profile data.
- Run `npm run clear-db` to clear db data first
- Run `npm run seed-data` to import data into db


## Configuration

Configuration for the application is at `config/default.js`.
The following parameters can be set in config files or in env variables:

- LOG_LEVEL: the log level, default is 'debug'
- PORT: the server port, default is 3000
- AUTH_SECRET: The authorization secret used during token verification.
- VALID_ISSUERS: The valid issuer of tokens.
- AUTH0_URL: AUTH0 URL, used to get M2M token
- AUTH0_PROXY_SERVER_URL: AUTH0 proxy server URL, used to get M2M token
- AUTH0_AUDIENCE: AUTH0 audience, used to get M2M token
- TOKEN_CACHE_TIME: AUTH0 token cache time, used to get M2M token
- AUTH0_CLIENT_ID: AUTH0 client id, used to get M2M token
- AUTH0_CLIENT_SECRET: AUTH0 client secret, used to get M2M token
- BUSAPI_URL: Bus API URL
- KAFKA_ERROR_TOPIC: Kafka error topic used by bus API wrapper
- GROUPS_API_URL: Groups API URL
- AMAZON.AWS_ACCESS_KEY_ID: The Amazon certificate key to use when connecting.
- AMAZON.AWS_SECRET_ACCESS_KEY: The Amazon certificate access key to use when connecting.
- AMAZON.AWS.SESSION_TOKEN: The user session token, used when developing locally against the TC dev AWS services
- AMAZON.AWS_REGION: The Amazon certificate region to use when connecting.
- AMAZON.PHOTO_S3_BUCKET: the AWS S3 bucket to store photos
- AMAZON.S3_API_VERSION: the AWS S3 API version
- FILE_UPLOAD_SIZE_LIMIT: the file upload size limit in bytes
- PHOTO_URL_TEMPLATE: photo URL template, its <key> will be replaced with S3 object key
- VERIFY_TOKEN_EXPIRATION: verify token expiration in minutes
- EMAIL_VERIFY_AGREE_URL: email verify agree URL, the <emailVerifyToken> will be replaced with generated verify token
- EMAIL_VERIFY_DISAGREE_URL: email verify disagree URL
- SCOPES: the configurable M2M token scopes, refer `config/default.js` for more details
- MEMBER_SECURE_FIELDS: Member profile identifiable info fields, only admin, M2M, or member himself can fetch these fields
- COMMUNICATION_SECURE_FIELDS: Member contact information, accessible by admins, managers, and copilots - anyone with a role in the AUTOCOMPLETE_ROLES array
- MEMBER_TRAIT_SECURE_FIELDS: Member traits identifiable info fields, only admin, M2M, or member himself can fetch these fields
- MISC_SECURE_FIELDS: Misc identifiable info fields, only admin, M2M, or member himself can fetch these fields
- STATISTICS_SECURE_FIELDS: Member Statistics identifiable info fields, only admin, M2M, or member himself can fetch these fields
- HEALTH_CHECK_TIMEOUT: health check timeout in milliseconds

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
- Install dependencies `npm install`
- Start app `npm start`
- App is running at port 3000. You can visit `http://localhost:3000/v6/members/health`


## Tests


Make sure you have followed above steps to
- setup db and config db url
- setup local mock api and set local configs
  - it will really call service and mock api


Then you can run:
```bash
npm run test
```

## Verification
Refer to the verification document `Verification.md`
