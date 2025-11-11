import { ManyToOne, PrimaryKey, Property } from '@mikro-orm/core';
import { Employer } from 'src/user/entities/employer.entity';
import { getDatefromDifference } from 'src/utils';

export abstract class CommonEmployerSubscription {
  @PrimaryKey()
  id!: number;

  @ManyToOne(() => Employer)
  employer: Employer;

  @Property()
  validForDays: number;

  @Property()
  createdAt: Date;

  @Property({ onUpdate: () => new Date() })
  updatedAt = new Date();

  @Property()
  expiryDate: Date;

  constructor({
    validForDays,
    employer,
  }: {
    validForDays: number;
    employer: Employer;
  }) {
    this.validForDays = validForDays;
    this.expiryDate = getDatefromDifference(this.validForDays);
    this.createdAt = new Date();
    this.employer = employer;
  }
}
