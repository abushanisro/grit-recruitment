import { Entity, ManyToOne, Property } from '@mikro-orm/core';
import { Job } from './job.entity';
import { User } from '../../user/entities/user.entity';

@Entity()
export class SentJobAlert {
  @ManyToOne({ entity: () => Job, primary: true })
  job: Job;

  @ManyToOne({ entity: () => User, primary: true })
  user: User;

  @Property()
  createdAt = new Date();

  @Property({ onUpdate: () => new Date() })
  updatedAt = new Date();

  constructor({ job, user }: { user: User; job: Job }) {
    this.job = job;
    this.user = user;
  }
}
