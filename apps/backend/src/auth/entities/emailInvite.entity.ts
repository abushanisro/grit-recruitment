import { Entity, ManyToOne, Property } from '@mikro-orm/core';
import { User } from '../../user/entities/user.entity';
import { BaseEntity } from '../../base.entity';

@Entity()
export class EmailInvite extends BaseEntity {
  @ManyToOne()
  user: User;

  @Property()
  refId: string;

  @Property()
  isUsed: boolean = false;

  constructor({ user, refId }: { user: User; refId: string }) {
    super();
    this.user = user;
    this.refId = refId;
  }
}
