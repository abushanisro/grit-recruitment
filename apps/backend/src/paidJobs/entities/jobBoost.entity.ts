import {
  Entity,
  ManyToOne,
  PrimaryKey,
  Property,
  Unique,
} from '@mikro-orm/core';
import { Job } from 'src/job/entities/job.entity';
import { EmployerPaidJobSubscription } from './employerPaidJobSubscription.entity';
import { Employer } from 'src/user/entities/employer.entity';
import { v4 } from 'uuid';

@Entity()
@Unique({ properties: ['job', 'subscription'] })
export class JobBoost {
  @PrimaryKey()
  id: string;

  @ManyToOne({ entity: () => Job, nullable: true })
  job: Job | undefined;

  @ManyToOne(() => EmployerPaidJobSubscription)
  subscription: EmployerPaidJobSubscription;

  @ManyToOne(() => Employer)
  employer: Employer;

  @Property()
  boostStartDate: Date | null;

  @Property()
  boostEndDate: Date | null;

  @Property()
  validFor: number;

  @Property()
  points: number;

  @Property({ default: false })
  isDeleted: boolean;

  constructor({
    job,
    subscription,
    employer,
    boostStartDate,
    boostEndDate,
    isDeleted,
  }: {
    job?: Job;
    subscription: EmployerPaidJobSubscription;
    employer: Employer;
    boostStartDate: Date | null;
    boostEndDate: Date | null;
    isDeleted?: boolean;
  }) {
    this.id = v4();
    this.job = job;
    this.subscription = subscription;
    this.employer = employer;
    this.boostStartDate = boostStartDate;
    this.boostEndDate = boostEndDate;
    this.points = subscription.points;
    this.validFor = subscription.validForDays;
    this.isDeleted = isDeleted || false;
  }
}
