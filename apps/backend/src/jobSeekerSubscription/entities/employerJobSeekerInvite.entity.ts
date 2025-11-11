import { Entity, ManyToOne } from '@mikro-orm/core';
import { Job } from 'src/job/entities/job.entity';
import { CommonJobInvite } from 'src/subscription/baseEntities/jobInvite.entity';
import { Employer } from 'src/user/entities/employer.entity';
import { JobSeeker } from 'src/user/entities/jobseeker.entity';

@Entity()
export class EmployerJobSeekerInvite extends CommonJobInvite {
  @ManyToOne(() => JobSeeker)
  jobSeeker: JobSeeker;

  @ManyToOne(() => Job)
  job: Job;

  constructor({
    employer,
    jobSeeker,
    job,
  }: {
    employer: Employer;
    jobSeeker: JobSeeker;
    job: Job;
  }) {
    super({ employer });
    this.jobSeeker = jobSeeker;
    this.job = job;
  }
}
