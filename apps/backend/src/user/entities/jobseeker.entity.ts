import {
  Collection,
  Entity,
  Enum,
  ManyToMany,
  ManyToOne,
  OneToOne,
  Property,
} from '@mikro-orm/core';
import { City, Language, Skill, Subfunction } from './info.entity';
import { User } from './user.entity';
import { EmploymentMode, NoticePeriodEnum } from 'contract/enum';

@Entity()
export class JobSeeker {
  @OneToOne({ entity: () => User, primary: true })
  user: User;

  @Property()
  createdAt = new Date();

  @Property({ onUpdate: () => new Date() })
  updatedAt = new Date();

  @Property({ type: 'text' })
  profileSummary: string;

  @Property()
  headline: string;

  @Property({ nullable: true })
  expectedSalaryInLpa: number | null;

  @Property()
  isPrivate: boolean;

  @Property({ nullable: true, type: 'text' })
  externalLink: string | null;

  @Property({ type: 'text' })
  resume: string;

  @Property({ type: 'text' })
  videoResume: string;

  @ManyToMany()
  preferredLocations = new Collection<City>(this);

  @Property({ type: 'double precision' })
  experienceInYear: number;

  @ManyToMany()
  languages = new Collection<Language>(this);

  @ManyToMany()
  subfunction = new Collection<Subfunction>(this);

  @Enum({ items: () => NoticePeriodEnum })
  noticePeriod: NoticePeriodEnum;

  @Enum({ items: () => EmploymentMode, nullable: true })
  workSchedule: EmploymentMode | null;

  @ManyToOne()
  city: City;

  @ManyToMany()
  skills = new Collection<Skill>(this);

  @Property({ nullable: true, type: 'text' })
  socialMediaLink: string | null;

  @Property()
  isSubscribedToAlerts: boolean = false;

  constructor({
    user,
    headline,
    expectedSalaryInLpa,
    isPrivate,
    externalLink,
    resume,
    videoResume,
    profileSummary,
    preferredLocations,
    experienceInYear,
    languages,
    subfunction,
    noticePeriod,
    workSchedule,
    city,
    skills,
    socialMediaLink,
    isSubscribedToAlerts,
  }: {
    user: User;
    profileSummary: string;
    headline: string;
    expectedSalaryInLpa: number | null;
    isPrivate: boolean;
    externalLink: string | null;
    resume: string;
    videoResume: string;
    preferredLocations: City[];
    experienceInYear: number;
    languages: Language[];
    subfunction: Subfunction[];
    noticePeriod: NoticePeriodEnum;
    workSchedule: EmploymentMode | null;
    city: City;
    skills: Skill[];
    socialMediaLink: string | null;
    isSubscribedToAlerts: boolean;
  }) {
    this.user = user;
    this.profileSummary = profileSummary;
    this.headline = headline;
    this.expectedSalaryInLpa = expectedSalaryInLpa;
    this.isPrivate = isPrivate;
    this.externalLink = externalLink;
    this.resume = resume;
    this.videoResume = videoResume;
    this.preferredLocations.set(preferredLocations);
    this.languages.set(languages);
    this.subfunction.set(subfunction);
    this.experienceInYear = experienceInYear;
    this.noticePeriod = noticePeriod;
    this.workSchedule = workSchedule;
    this.city = city;
    this.skills.set(skills);
    this.socialMediaLink = socialMediaLink;
    this.isSubscribedToAlerts = isSubscribedToAlerts;
  }
}
