import { Entity, ManyToOne, Property, Unique } from '@mikro-orm/core';
import { Employer } from 'src/user/entities/employer.entity';
import { EmployerJobSeekerSubscription } from './employerJobSeekerSubscription.entity';
import { JobSeeker } from 'src/user/entities/jobseeker.entity';
import { CommonProfileViews } from 'src/subscription/baseEntities/profileViews.entity';

@Entity()
@Unique({ properties: ['employer', 'jobSeeker', 'employerSubscription'] })
export class EmployerProfileViews extends CommonProfileViews {
  @ManyToOne(() => JobSeeker)
  jobSeeker: JobSeeker;

  @Property()
  jobSeekerData: JSON;

  @ManyToOne(() => EmployerJobSeekerSubscription)
  employerSubscription: EmployerJobSeekerSubscription;

  constructor({
    employer,
    jobSeeker,
    employerSubscription,
    jobSeekerData,
  }: {
    employer: Employer;
    jobSeeker: JobSeeker;
    jobSeekerData: JSON;
    employerSubscription: EmployerJobSeekerSubscription;
  }) {
    super({ employer });
    this.jobSeeker = jobSeeker;
    this.employerSubscription = employerSubscription;
    this.jobSeekerData = jobSeekerData;
  }
}
