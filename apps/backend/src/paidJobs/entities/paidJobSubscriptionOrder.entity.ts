import { Entity, ManyToOne, Property } from '@mikro-orm/core';
import { PaidJobSubscriptionPlan } from './paidJobSubscriptionPlan.entity';
import { Employer } from 'src/user/entities/employer.entity';
import { CommonSubscriptionOrder } from 'src/subscription/baseEntities/subscriptionOrder.entity';

@Entity()
export class PaidJobSubscriptionOrder extends CommonSubscriptionOrder {
  @ManyToOne(() => PaidJobSubscriptionPlan)
  plan: PaidJobSubscriptionPlan;

  @Property()
  planName: string;

  @Property()
  boostLimit: number;

  @Property()
  boostDays: number;

  constructor({
    employer,
    plan,
    razorpayOrderId,
    razorpayInvoiceId,
    invoiceLink,
  }: {
    employer: Employer;
    plan: PaidJobSubscriptionPlan;
    razorpayOrderId: string;
    razorpayInvoiceId: string;
    invoiceLink: string;
  }) {
    super({
      employer,
      invoiceLink,
      paidAmount: plan.price,
      razorpayInvoiceId,
      razorpayOrderId,
      validForDays: plan.validForDays,
    });
    this.plan = plan;
    this.planName = plan.name;
    this.boostLimit = plan.boostLimit;
    this.boostDays = plan.boostDays;
  }
}
