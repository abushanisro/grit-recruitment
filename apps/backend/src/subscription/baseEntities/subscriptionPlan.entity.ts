import { PrimaryKey, Property } from '@mikro-orm/core';

export abstract class CommonSubscriptionPlan {
  @PrimaryKey()
  id: string;

  @Property()
  price: number;

  @Property()
  name: string;

  @Property()
  validForDays: number;

  @Property({ type: 'text' })
  description: string;

  constructor({
    id,
    name,
    validForDays,
    description,
    price,
  }: {
    id: string;
    name: string;
    validForDays: number;
    description: string;
    price: number;
  }) {
    this.id = id;
    this.name = name;
    this.price = price;
    this.validForDays = validForDays;
    this.description = description;
  }
}
