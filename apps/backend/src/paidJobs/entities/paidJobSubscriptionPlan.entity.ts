import { Entity, Property } from '@mikro-orm/core';
import { CommonSubscriptionPlan } from 'src/subscription/baseEntities/subscriptionPlan.entity';

@Entity()
export class PaidJobSubscriptionPlan extends CommonSubscriptionPlan {
  @Property()
  boostLimit: number;

  @Property()
  boostDays: number;

  @Property({ type: 'text' })
  description: string;

  @Property()
  points: number;

  constructor({
    id,
    name,
    validForDays,
    boostLimit,
    boostDays,
    description,
    price,
    points,
  }: {
    id: string;
    name: string;
    validForDays: number;
    boostLimit: number;
    boostDays: number;
    description: string;
    price: number;
    points: number;
  }) {
    super({ id, name, validForDays, description, price });
    this.id = id;
    this.name = name;
    this.price = price;
    this.validForDays = validForDays;
    this.boostLimit = boostLimit;
    this.boostDays = boostDays;
    this.description = description;
    this.points = points;
  }
}
