import {
  Collection,
  Entity,
  Enum,
  ManyToMany,
  Property,
} from '@mikro-orm/core';
import { Industry } from '../../user/entities/info.entity';
import { BaseEntity } from '../../base.entity';
import { CompanySizeEnum } from '../../../../contract/enum';

@Entity()
export class Company extends BaseEntity {
  @Property()
  name: string;

  @Property({ type: 'text', nullable: true })
  website: string | null;

  @ManyToMany()
  industries = new Collection<Industry>(this);

  @Property({ type: 'text', length: 2000 })
  aboutCompany: string;

  @Enum({ items: () => CompanySizeEnum, nullable: true })
  companySize: CompanySizeEnum | null;

  @Property({ nullable: true, type: 'text' })
  logo: string | null;

  @Property({ type: 'text' })
  gstNumber: string;

  @Property({ type: 'text' })
  gstAddress: string;

  constructor({
    name,
    website,
    industries,
    aboutCompany,
    companySize,
    logo,
    gstNumber,
    gstAddress,
  }: {
    name: string;
    website: string | null;
    industries: Industry[];
    aboutCompany: string;
    companySize: CompanySizeEnum | null;
    logo: string | null;
    gstNumber: string;
    gstAddress: string;
  }) {
    super();
    this.name = name;
    this.website = website || null;
    this.industries.add(industries);
    this.aboutCompany = aboutCompany;
    this.companySize = companySize;
    this.logo = logo || null;
    this.gstNumber = gstNumber;
    this.gstAddress = gstAddress;
  }
}
