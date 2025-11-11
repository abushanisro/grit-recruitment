import { Module } from '@nestjs/common';
import { AnalyticEventService } from './analytic-event.service';
import { AnalyticEventController } from './analytic-event.controller';

@Module({
  controllers: [AnalyticEventController],
  providers: [AnalyticEventService]
})
export class AnalyticEventModule {}
