import { Module, Global, DynamicModule, Provider } from '@nestjs/common';
import PgBoss = require('pg-boss');
import { DiscoveryModule } from '@golevelup/nestjs-discovery';
import { logger } from '@mikro-orm/nestjs';
import { QueueService } from './queue.service';

@Global()
@Module({
  imports: [DiscoveryModule],
  providers: [QueueService],
  exports: [QueueService],
})
export class PgscheduleModule {
  static forRoot(options: PgBoss.ConstructorOptions): DynamicModule {
    const pgBossProvider: Provider = {
      provide: 'PG_BOSS',
      useFactory: async (): Promise<PgBoss> => {
        const boss = new PgBoss(options);
        boss.on('error', (error) => {
          logger.error(error);
        });
        boss.on('monitor-states', (monitorState) => {
          logger.log(monitorState);
        });
        const isTest = process.env.NODE_ENV === 'test';
        if (!isTest) {
          await boss.start();
        }
        return boss;
      },
    };

    const providers = [pgBossProvider, QueueService];
    return {
      imports: [DiscoveryModule],
      module: PgscheduleModule,
      providers: providers,
      exports: providers,
    };
  }
}
