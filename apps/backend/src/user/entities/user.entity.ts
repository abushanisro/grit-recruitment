import {
  Entity,
  OneToOne,
  Property,
  Unique,
  Enum,
  Formula,
} from '@mikro-orm/core';
import { BaseEntity } from '../../base.entity';
import { Employer } from './employer.entity';
import { JobSeeker } from './jobseeker.entity';
import { UserRole } from 'contract/enum';

@Entity()
export class User extends BaseEntity {
  @Property()
  firstName: string;

  @Property()
  lastName: string;

  @Property()
  @Unique()
  email: string;

  @Property()
  @Unique()
  phone: string;

  @Enum({ items: () => UserRole })
  role: UserRole;

  @Property({ type: 'text', nullable: true })
  picture: string | null;

  @OneToOne(() => JobSeeker, (e) => e.user, { nullable: true })
  jobSeeker!: JobSeeker | null;

  @OneToOne(() => Employer, (e) => e.user, { nullable: true })
  employer!: Employer | null;

  @Formula("first_name || ' ' || last_name")
  fullName!: string;

  constructor({
    firstName,
    lastName,
    email,
    role,
    phone,
    picture,
  }: {
    firstName: string;
    lastName: string;
    email: string;
    role: UserRole;
    phone: string;
    picture: string | null;
  }) {
    super();
    this.firstName = firstName;
    this.lastName = lastName;
    this.email = email;
    this.phone = phone;
    this.role = role;
    this.picture = picture;
  }
}
