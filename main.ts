import 'reflect-metadata';

import { NestFactory } from '@nestjs/core';
import { ExpressAdapter } from '@nestjs/platform-express';
import { AppModule } from './src/app.module';

const config = require('config');
const expressApplication = require('./app');
const logger = require('./src/common/logger');

/**
 * Bootstraps Member API v6 through NestJS using the compatibility Express app.
 *
 * Nest's automatic body parser is disabled because the established application
 * already registers JSON, URL-encoded, file-upload, authentication, and error
 * middleware in a behavior-sensitive order. The existing PORT environment
 * variable remains the only listener configuration.
 *
 * @returns A promise that resolves after the HTTP server begins listening.
 * @throws Propagates Nest application creation or listener startup failures.
 */
export async function bootstrap(): Promise<void> {
  const adapter = new ExpressAdapter(expressApplication);
  const app = await NestFactory.create(AppModule, adapter, {
    bodyParser: false,
    logger: false,
  });

  await app.listen(config.PORT);
  logger.info(`NestJS server listening on port ${config.PORT}`);
}

if (require.main === module) {
  void bootstrap();
}
