import { Entity, ManyToOne, Property } from '@mikro-orm/core';
import { BaseEntity } from 'src/base.entity';
import { User } from 'src/user/entities/user.entity';

@Entity()
export class AnalyticEvent extends BaseEntity {
  @ManyToOne()
  user: User;

  @Property()
  eventName: string;

  @Property({ type: 'text' })
  pageUrl: string;

  @Property()
  device: string;

  @Property()
  browser: string;

  @Property()
  userAgent: string;

  constructor({
    user,
    eventName,
    pageUrl,
    device,
    browser,
    userAgent,
  }: {
    user: User;
    eventName: string;
    pageUrl: string;
    device: string;
    browser: string;
    userAgent: string;
  }) {
    super();
    this.user = user;
    this.eventName = eventName;
    this.pageUrl = pageUrl;
    this.device = device;
    this.browser = browser;
    this.userAgent = userAgent;
  }
}
