import { ManyToOne, PrimaryKey, Property } from '@mikro-orm/core';
import { Employer } from 'src/user/entities/employer.entity';
import { v4 } from 'uuid';

export abstract class CommonProfileViews {
  @PrimaryKey()
  @Property()
  id!: string;

  @Property()
  createdAt: Date = new Date();

  @Property({ onUpdate: () => new Date() })
  updatedAt: Date = new Date();

  @ManyToOne(() => Employer)
  employer: Employer;

  constructor({ employer }: { employer: Employer }) {
    this.employer = employer;
    this.id = v4();
  }
}
