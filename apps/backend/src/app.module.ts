import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { UserModule } from './user/user.module';
import { AuthModule } from './auth/auth.module';
import { ConfigModule, ConfigService } from '@nestjs/config';
import configuration from './configuration/configuration';
import { validate } from './configuration/env.validation';
import { JobModule } from './job/job.module';
import { UploadModule } from './upload/upload.module';
import { PgscheduleModule } from './pgschedule/pgschedule.module';
import { APP_INTERCEPTOR } from '@nestjs/core';
import { JobInterceptor } from './job/job.interceptor';
import { AnalyticEventModule } from './analytic-event/analytic-event.module';
import { LoggerModule } from 'nestjs-pino';
/* eslint-disable @typescript-eslint/naming-convention */
const pinoMultiStream = require('pino-multi-stream').multistream;
/* eslint-enable @typescript-eslint/naming-convention */
import { v4 } from 'uuid';
import { CamelCasePlugin, PostgresDialect } from 'kysely';
import { Pool } from 'pg';
import { KyselyModule } from 'nestjs-kysely';
import { AlertsModule } from './alerts/alert.module';
import { PaidJobsModule } from './paidJobs/paidJobs.module';
import { JobSeekerSubscriptionModule } from './jobSeekerSubscription/jobSeekerSubscription.module';

const testDbUrl = 'postgresql://postgres:abc@localhost:5435/postgres';

const getKyselyConfig = async () => {
  const isTest = process.env.NODE_ENV === 'test';
  if (isTest) {
    return {
      dialect: new PostgresDialect({
        pool: new Pool({
          connectionString: testDbUrl,
        }),
      }),
      plugins: [new CamelCasePlugin()],
    };
  }
  return {
    dialect: new PostgresDialect({
      pool: new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: {
          rejectUnauthorized: false,
        },
      }),
    }),
    plugins: [new CamelCasePlugin()],
  };
};
@Module({
  imports: [
    MikroOrmModule.forRoot(),
    UserModule,
    AuthModule,
    PaidJobsModule,
    JobSeekerSubscriptionModule,
    ConfigModule.forRoot({ isGlobal: true, load: [configuration], validate }),
    JobModule,
    UploadModule,
    PgscheduleModule.forRoot({
      connectionString: process.env.DATABASE_URL as string,
      ssl: {
        rejectUnauthorized: false,
      },
    }),
    KyselyModule.forRootAsync({
      useFactory: getKyselyConfig,
    }),
    AnalyticEventModule,
    LoggerModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        const env = configService.get('NODE_ENV');
        const streams = [{ stream: process.stdout }];
        return {
          pinoHttp: [
            {
              genReqId: () => v4().replace(/-/g, ''),
              ...(env === 'development'
                ? {
                    transport: {
                      target: 'pino-pretty',
                      options: { colorize: true },
                    },
                  }
                : {}),
            },
            pinoMultiStream(streams),
          ],
        };
      },
    }),
    AlertsModule,
  ],
  controllers: [AppController],
  providers: [
    AppService,
    {
      provide: APP_INTERCEPTOR,
      useClass: JobInterceptor,
    },
  ],
})
export class AppModule {}
