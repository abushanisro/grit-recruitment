import { Entity, Enum, ManyToOne, Property, Unique } from '@mikro-orm/core';
import { Job } from './job.entity';
import { JobSeeker } from 'src/user/entities/jobseeker.entity';
import { JobApplicationStatus } from 'contract/enum';
import { BaseEntity } from 'src/base.entity';

@Entity()
@Unique({ properties: ['jobSeeker', 'job'] })
export class JobApplication extends BaseEntity {
  @ManyToOne()
  jobSeeker: JobSeeker;

  @ManyToOne()
  job: Job;

  @Enum({ items: () => JobApplicationStatus })
  status: JobApplicationStatus;

  @Property({ nullable: true, default: null })
  statusChangedTime: Date | null = null;

  @Property({ length: 2000, default: null, nullable: true })
  coverLetter: string | null = null;

  @Property()
  areYouOkayWithTheLocation: boolean;

  constructor({
    jobSeeker,
    job,
    coverLetter,
    areYouOkayWithTheLocation,
  }: {
    jobSeeker: JobSeeker;
    job: Job;
    coverLetter: string | null;
    areYouOkayWithTheLocation: boolean;
  }) {
    super();
    this.jobSeeker = jobSeeker;
    this.job = job;
    this.areYouOkayWithTheLocation = areYouOkayWithTheLocation;
    this.coverLetter = coverLetter;
    this.status = JobApplicationStatus.Pending;
  }
}
