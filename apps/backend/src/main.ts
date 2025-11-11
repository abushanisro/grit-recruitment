import { contract } from 'contract';
import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { generateOpenApi } from '@ts-rest/open-api';
import { SwaggerModule } from '@nestjs/swagger';
import { Logger } from 'nestjs-pino';
import { json } from 'body-parser';
// eslint-disable-next-line
const pg = require('pg');

async function bootstrap() {
  const app = await NestFactory.create(AppModule, {
    cors: true,
    bufferLogs: true,
  });
  app.use(json({ limit: '5mb' }));
  app.enableCors();
  app.useLogger(app.get(Logger));
  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      transformOptions: { enableImplicitConversion: true },
    }),
  );
  pg.types.setTypeParser(20, 'text', parseInt);

  const document = generateOpenApi(contract, {
    info: {
      title: 'Jobverse API',
      version: '1.0.0',
    },
  });

  SwaggerModule.setup('swagger', app, document);

  await app.listen(process.env['PORT'] || 3000);
}
void bootstrap();
