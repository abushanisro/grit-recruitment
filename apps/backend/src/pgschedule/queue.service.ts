import { Injectable, Inject, OnModuleInit } from '@nestjs/common';
import { QUEUE_META, SCHEDULE_META } from './pgschedule.decorator';
import PgBoss, { Job } from 'pg-boss';
import {
  DiscoveryService,
  DiscoveredMethodWithMeta,
} from '@golevelup/nestjs-discovery';
import { QueueNamesEnum, QueuePayloadData } from './config/queueConfig';
import { ConfigService } from '@nestjs/config';
import { MikroORM, CreateRequestContext } from '@mikro-orm/core';

@Injectable()
export class QueueService implements OnModuleInit {
  constructor(
    @Inject('PG_BOSS') private boss: PgBoss,

    private readonly discover: DiscoveryService,

    private readonly configService: ConfigService,

    // @ts-expect-error todo: fix types
    private readonly orm: MikroORM,
  ) {}

  env = this.configService.get<string>('environment')!;

  queueOptions = {
    retryLimit: Number(process.env.QUEUE_RETRY_LIMIT!),
    retentionDays: Number(process.env.QUEUE_RETENTION_DAYS!),
  };

  public async onModuleInit(): Promise<void> {
    const messageHandlers = await this.discover.providerMethodsWithMetaAtKey<{
      name: string;
      cron: string;
    }>(SCHEDULE_META);

    const queueHandlers = await this.discover.providerMethodsWithMetaAtKey<{
      name: string;
    }>(QUEUE_META);

    await Promise.all(
      messageHandlers.map(async (handler) => {
        const metadata = {
          ...handler.meta,
          name: this.env + '-' + handler.meta.name,
        };

        await this.boss.schedule(metadata.name, metadata.cron, {});
        this.handler(metadata, handler);
      }),
    );

    await Promise.all(
      queueHandlers.map(async (handler) => {
        const metadata = {
          name: this.env + '-' + handler.meta.name,
        };

        this.handler(metadata, handler);
      }),
    );
  }

  @CreateRequestContext() // Replace with CreateRequestContext when using MikroORM v6 and above
  handler(
    metadata: { name: string },
    handler: DiscoveredMethodWithMeta<{
      name: string;
    }>,
  ) {
    void this.boss.work(
      metadata.name,
      async (job: Job<QueuePayloadData<QueueNamesEnum>>) => {
        await handler.discoveredMethod.handler.call(
          handler.discoveredMethod.parentClass.instance,
          {
            name: job.data.name,
            data: job.data.data,
          } satisfies QueuePayloadData<QueueNamesEnum>,
        );
      },
    );
  }

  public async sendMessage(
    options: QueuePayloadData<QueueNamesEnum>,
  ): Promise<void> {
    await this.boss.send(
      this.env + '-' + options.name,
      options,
      this.queueOptions,
    );
  }

  public async onModuleDestroy(): Promise<void> {
    await this.boss.stop();
  }
}
