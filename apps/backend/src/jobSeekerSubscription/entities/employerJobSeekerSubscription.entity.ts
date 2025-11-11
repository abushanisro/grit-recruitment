import { Entity, Property, ManyToOne, OneToOne } from '@mikro-orm/core';
import { JobSeekerSubscriptionPlan } from './jobSeekerSubscriptionPlan.entity';
import { JobSeekerSubscriptionOrder } from './jobSeekerSubscriptionOrder.entity';
import { CommonEmployerSubscription } from 'src/subscription/baseEntities/employerSubscription.entity';

@Entity()
export class EmployerJobSeekerSubscription extends CommonEmployerSubscription {
  @Property()
  allowedProfileViewCount: number;

  @OneToOne(() => JobSeekerSubscriptionOrder)
  subscriptionOrder: JobSeekerSubscriptionOrder;

  @ManyToOne(() => JobSeekerSubscriptionPlan)
  plan: JobSeekerSubscriptionPlan;

  constructor({ order }: { order: JobSeekerSubscriptionOrder }) {
    super({ employer: order.employer, validForDays: order.validForDays });
    this.allowedProfileViewCount = order.allowedProfileCount;
    this.subscriptionOrder = order;
    this.plan = order.plan;
  }
}
