import { Entity, Property } from '@mikro-orm/core';
import { CommonSubscriptionPlan } from 'src/subscription/baseEntities/subscriptionPlan.entity';

@Entity()
export class JobSeekerSubscriptionPlan extends CommonSubscriptionPlan {
  @Property()
  allowedProfileCount: number;

  constructor({
    name,
    price,
    allowedProfileCount,

    description,
    validForDays,
    id,
  }: {
    allowedProfileCount: number;
    description: string;

    name: string;
    price: number;
    validForDays: number;
    id: string;
  }) {
    super({ id, name, validForDays, description, price });
    this.allowedProfileCount = allowedProfileCount;
    this.description = description;
    this.name = name;
    this.id = id;
    this.price = price;
    this.validForDays = validForDays;
  }
}
