import { Entity, ManyToOne, Property } from '@mikro-orm/core';
import { User } from '../../user/entities/user.entity';
import { Job } from './job.entity';

@Entity()
export class SavedJob {
  @ManyToOne({ entity: () => User, primary: true })
  user: User;

  @ManyToOne({ entity: () => Job, primary: true })
  job: Job;

  @Property({ default: false })
  isDeleted: boolean = false;

  @Property()
  createdAt = new Date();

  @Property({ onUpdate: () => new Date() })
  updatedAt = new Date();

  constructor({
    user,
    job,
    isDeleted,
  }: {
    user: User;
    job: Job;
    isDeleted: boolean;
  }) {
    this.user = user;
    this.job = job;
    this.isDeleted = isDeleted || false;
  }
}
