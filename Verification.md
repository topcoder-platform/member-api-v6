
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

