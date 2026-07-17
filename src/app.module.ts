import { Module } from '@nestjs/common';

/**
 * Root NestJS module for Member API v6.
 *
 * HTTP behavior remains registered on the compatibility Express application
 * and is mounted through Nest's Express adapter. This keeps the public API,
 * Joi validation, authentication, middleware ordering, and response contracts
 * stable while NestJS owns application startup and lifecycle.
 */
@Module({})
export class AppModule {}
