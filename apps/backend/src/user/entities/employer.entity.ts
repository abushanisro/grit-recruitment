import { Entity, OneToOne, Unique, Property, ManyToOne } from '@mikro-orm/core';
import { Company } from '../../job/entities/company.entity';
import { City } from './info.entity';
import { User } from './user.entity';

@Entity()
export class Employer {
  @Unique()
  @OneToOne({ entity: () => User, primary: true })
  user: User;

  @Property()
  createdAt = new Date();

  @Property({ onUpdate: () => new Date() })
  updatedAt = new Date();

  @ManyToOne()
  company: Company;

  @Property({ type: 'text' })
  verificationDocument: string;

  @Property()
  isVerified: boolean = false;

  @ManyToOne({ nullable: true })
  city: City | null;

  constructor({
    user,
    company,
    verificationDocument,
    city,
  }: {
    user: User;
    company: Company;
    verificationDocument: string;
    city: City | null;
  }) {
    this.user = user;
    this.company = company;
    this.verificationDocument = verificationDocument;
    this.city = city;
  }
}
