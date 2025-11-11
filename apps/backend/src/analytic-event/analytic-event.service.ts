import { EntityManager } from '@mikro-orm/postgresql';
import { Injectable } from '@nestjs/common';
import { User } from 'src/user/entities/user.entity';
import { AnalyticEventShapes } from './analytic-event.controller';
import { AnalyticEvent } from './entities/analytic-event.entity';

@Injectable()
export class AnalyticEventService {
  constructor(private em: EntityManager) {}

  async createAnalyticEvent(
    user: User,
    dto: AnalyticEventShapes['createAnalyticEvent']['body'],
  ) {
    const analyticalEvent = new AnalyticEvent({
      user: user,
      eventName: dto.eventName,
      pageUrl: dto.pageUrl,
      device: dto.device,
      browser: dto.browser,
      userAgent: dto.userAgent,
    });
    await this.em.persistAndFlush(analyticalEvent);
  }
}
