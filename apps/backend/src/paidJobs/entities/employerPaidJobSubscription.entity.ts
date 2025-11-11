import {
  Collection,
  Entity,
  ManyToOne,
  OneToMany,
  OneToOne,
  Property,
} from '@mikro-orm/core';
import { PaidJobSubscriptionOrder } from './paidJobSubscriptionOrder.entity';
import { PaidJobSubscriptionPlan } from './paidJobSubscriptionPlan.entity';
import { CommonEmployerSubscription } from 'src/subscription/baseEntities/employerSubscription.entity';
import { JobBoost } from './jobBoost.entity';

@Entity()
export class EmployerPaidJobSubscription extends CommonEmployerSubscription {
  @Property()
  planName: string;

  @Property()
  boostLimit: number;

  @Property()
  boostDays: number;

  @ManyToOne(() => PaidJobSubscriptionPlan)
  plan: PaidJobSubscriptionPlan;

  @OneToOne(() => PaidJobSubscriptionOrder)
  order: PaidJobSubscriptionOrder;

  @Property()
  points: number;

  // Todo: make it hidden
  @OneToMany(() => JobBoost, (jobBoost) => jobBoost.subscription)
  jobBoosts = new Collection<JobBoost>(this);

  get activeJobBoosts() {
    return this.jobBoosts
      .getItems()
      .filter(
        (b) =>
          (b.boostEndDate === null || b.boostEndDate > new Date()) &&
          !b.isDeleted,
      );
  }

  get usedJobBoosts() {
    return this.jobBoosts.getItems().filter((b) => !b.isDeleted);
  }

  constructor({ order }: { order: PaidJobSubscriptionOrder }) {
    super({ employer: order.employer, validForDays: order.validForDays });
    this.planName = order.planName;
    this.boostLimit = order.boostLimit;
    this.boostDays = order.boostDays;
    this.plan = order.plan;
    this.order = order;
    this.points = order.plan.points;
  }
}
