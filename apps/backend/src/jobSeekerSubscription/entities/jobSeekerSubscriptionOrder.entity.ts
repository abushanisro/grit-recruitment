import { Entity, Property, ManyToOne } from '@mikro-orm/core';
import { Employer } from 'src/user/entities/employer.entity';
import { JobSeekerSubscriptionPlan } from './jobSeekerSubscriptionPlan.entity';
import { CommonSubscriptionOrder } from 'src/subscription/baseEntities/subscriptionOrder.entity';

@Entity()
export class JobSeekerSubscriptionOrder extends CommonSubscriptionOrder {
  @Property()
  allowedProfileCount: number;

  @ManyToOne(() => JobSeekerSubscriptionPlan)
  plan: JobSeekerSubscriptionPlan;

  constructor({
    employer,
    razorpayOrderId,
    razorpayInvoiceId,
    invoiceLink,
    plan,
  }: {
    plan: JobSeekerSubscriptionPlan;
    razorpayOrderId: string;
    razorpayInvoiceId: string;
    invoiceLink: string;
    employer: Employer;
  }) {
    super({
      employer,
      razorpayOrderId,
      razorpayInvoiceId,
      invoiceLink,
      paidAmount: plan.price,
      validForDays: plan.validForDays,
    });
    this.allowedProfileCount = plan.allowedProfileCount;
    this.plan = plan;
  }
}
