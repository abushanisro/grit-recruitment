import {
  Check,
  Collection,
  Entity,
  Enum,
  ManyToMany,
  ManyToOne,
  OneToMany,
  Property,
} from '@mikro-orm/core';
import { BaseEntity } from '../../base.entity';
import { Company } from './company.entity';
import { City, Industry, Subfunction } from '../../user/entities/info.entity';

import { Employer } from 'src/user/entities/employer.entity';
import { EmploymentMode, EmploymentType } from 'contract/enum';
import { JobBoost } from 'src/paidJobs/entities/jobBoost.entity';

@Entity()
@Check<Job>({
  name: 'admin_approve_check',
  expression: (columns) =>
    `not ((${columns.isAdminApproved}=true) and (${columns.adminApprovedTime} is null))`,
})
export class Job extends BaseEntity {
  @Property({ length: 150 })
  title: string;

  @ManyToOne({ nullable: true, entity: () => Company })
  company: Company | null;

  get companyName() {
    return this.isExternalJob
      ? this.externalJobCompanyName ?? ''
      : this.company?.name ?? '';
  }

  get companyLogo() {
    return this.company?.logo ?? null;
  }
  @ManyToMany()
  cities = new Collection<City>(this);

  @ManyToMany()
  industries = new Collection<Industry>(this);

  @ManyToMany()
  subFunctions = new Collection<Subfunction>(this);

  @Property({ nullable: true })
  description: JSON | null;

  @Property({ columnType: 'numeric', default: null, nullable: true })
  minSalaryInLpa: number | null = null;

  @Property({ columnType: 'numeric', default: null, nullable: true })
  maxSalaryInLpa: number | null = null;

  @Enum({ items: () => EmploymentType, array: true })
  employmentTypes: EmploymentType[];

  @Enum({ items: () => EmploymentMode, array: true })
  employmentModes: EmploymentMode[];

  @Property()
  minExperienceInYears: number;

  @Property()
  maxExperienceInYears: number;

  @ManyToOne({ nullable: true, entity: () => Employer })
  createdByEmployer: Employer | null;

  @Property({ default: false })
  isExternalJob: boolean = false;

  @Property({ nullable: true, default: null, length: 2000 })
  externalLink: string | null;

  @Property({ nullable: true, default: null })
  externalJobCompanyName: string | null;

  @Property({ default: false })
  isDeleted: boolean = false;

  @Property()
  isPosted: boolean = false;

  @Property({ nullable: true, default: null })
  postedTime: Date | null = null;

  @OneToMany(() => JobBoost, (jobBoost) => jobBoost.job)
  jobBoosts = new Collection<JobBoost>(this);

  get activeJobBoosts() {
    return this.jobBoosts
      .getItems()
      .filter(
        (b) =>
          (b.boostEndDate === null || b.boostEndDate > new Date()) &&
          !b.isDeleted,
      );
  }

  @Property({
    nullable: true,
    default: null,
    type: 'text',
  })
  searchVector: string | null = null;

  @Property({ default: false })
  isAdminApproved: boolean = false;

  @Property({ nullable: true, default: null })
  adminApprovedTime: Date | null = null;

  constructor({
    title,
    company,
    cities,
    industries,
    subFunctions,
    description,
    minSalaryInLpa,
    maxSalaryInLpa,
    employmentTypes,
    employmentModes,
    minExperienceInYears,
    maxExperienceInYears,
    createdByEmployer,
    isExternalJob,
    externalLink,
    externalJobCompanyName,
    isDeleted,
    searchVector,
    isPosted,
  }: {
    title: string;
    company: Company | null;
    cities: City[];
    industries: Industry[];
    subFunctions: Subfunction[];
    description: JSON | null;
    minSalaryInLpa: number | null;
    maxSalaryInLpa: number | null;
    employmentTypes: EmploymentType[];
    employmentModes: EmploymentMode[];
    minExperienceInYears: number;
    maxExperienceInYears: number;
    createdByEmployer: Employer | null;
    isExternalJob?: boolean;
    externalLink?: string | null;
    externalJobCompanyName?: string | null;
    isDeleted?: boolean;
    searchVector: string;
    isPosted: boolean;
  }) {
    super();
    this.title = title;
    this.company = company;
    this.cities.set(cities);
    this.industries.set(industries);
    this.subFunctions.set(subFunctions);
    this.description = description;
    this.minSalaryInLpa = minSalaryInLpa;
    this.maxSalaryInLpa = maxSalaryInLpa;
    this.employmentTypes = employmentTypes;
    this.employmentModes = employmentModes;
    this.minExperienceInYears = minExperienceInYears;
    this.maxExperienceInYears = maxExperienceInYears;
    this.createdByEmployer = createdByEmployer;
    this.isExternalJob = isExternalJob || false;
    this.externalLink = externalLink || null;
    this.externalJobCompanyName = externalJobCompanyName || null;
    this.isDeleted = isDeleted || false;
    this.searchVector = searchVector;
    this.isPosted = isPosted;
  }
}
