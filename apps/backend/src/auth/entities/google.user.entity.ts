import { Entity, Property, Unique } from '@mikro-orm/core';
import { BaseEntity } from '../../base.entity';

@Entity()
export class GoogleUser extends BaseEntity {
  @Property()
  firstName: string;

  @Property()
  lastName: string;

  @Property()
  email: string;

  @Property({ columnType: 'varchar' })
  picture: string;

  @Property()
  @Unique()
  referenceId: string;

  constructor({
    firstName,
    lastName,
    email,
    referenceId,
    picture,
  }: {
    firstName: string;
    lastName: string;
    email: string;
    referenceId: string;
    picture: string;
  }) {
    super();
    this.firstName = firstName;
    this.lastName = lastName;
    this.email = email;
    this.referenceId = referenceId;
    this.picture = picture;
  }
}
