import { PrimaryKey, Property, OptionalProps } from '@mikro-orm/core';

export abstract class BaseEntity<Optional = never> {
  @PrimaryKey()
  id!: number;

  @Property()
  createdAt = new Date();

  @Property({ onUpdate: () => new Date() })
  updatedAt = new Date();

  [OptionalProps]?: 'createdAt' | 'updatedAt' | Optional;
}
