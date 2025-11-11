import { BadRequestException } from '@nestjs/common';
import { generateSlugForIdPagePath } from 'contract/utils';
import { z } from 'zod';
import { Job } from './job/entities/job.entity';
import { isEmpty } from 'lodash';
import { JobBoost } from './paidJobs/entities/jobBoost.entity';
import { ExpectedSalaryInLpa, ExperienceInYear } from 'contract/enum';

export function handleZodValidationError(error: Readonly<z.ZodError>) {
  throw new BadRequestException(
    error.issues.length > 0
      ? `${error.issues[0]?.path[0]} ${error.issues[0]?.message}`
      : error.issues,
  );
}

export function calculateDateDifference(date1: Date, date2: Date): number {
  const oneDay = 24 * 60 * 60 * 1000; // hours*minutes*seconds*milliseconds
  const diffDays = Math.round(
    Math.abs((date1.getTime() - date2.getTime()) / oneDay),
  );
  return diffDays;
}

export function getDatefromDifference(
  dateDiff: number,
  initialDate = new Date(),
) {
  return new Date(new Date().setDate(initialDate.getDate() + dateDiff));
}

export function generateJobIdPageUrl(frontEndUrl: string, job: Job) {
  return `${frontEndUrl}/job/${job.id}/${generateSlugForIdPagePath([
    job.title,
    job.companyName,
    job.cities
      .getItems()
      .map((c) => c.name)
      .join('-'),
    job.employmentTypes.join('-'),
  ])}`;
}

export function createPaginatedResponse<T>({
  results,
  pageNumber,
  totalCount,
  pageSize,
}: {
  results: T[];
  pageNumber: number;
  totalCount: number;
  pageSize: number;
}) {
  return {
    currentPageNumber: pageNumber,
    currentPageSize: results.length,
    totalItems: totalCount,
    totalPages: Math.ceil(totalCount / pageSize),
    results,
  };
}

export function calculateBoostedDaysLeft(jobBoosts: JobBoost[]): number {
  const now = new Date();

  if (isEmpty(jobBoosts)) return 0;
  if (jobBoosts[0].boostEndDate === null)
    return jobBoosts[0].subscription.boostDays;
  return Math.ceil(
    (jobBoosts[0].boostEndDate.getTime() - now.getTime()) /
      (1000 * 60 * 60 * 24),
  );
}
export function isJobBoosted(activeJobs: JobBoost[]): boolean {
  return !isEmpty(activeJobs);
}

export function diffInMinutes(date1: Date, date2: Date): number {
  const diffInMilliseconds = Math.abs(date1.getTime() - date2.getTime());
  return diffInMilliseconds / (1000 * 60);
}

export function getExperienceFilterForJobSeeker(experience: ExperienceInYear) {
  switch (experience) {
    case ExperienceInYear.Fresher:
      return ['=', '0'];
    case ExperienceInYear.OneYears:
      return ['=', '1'];
    case ExperienceInYear.TwoYears:
      return ['=', '2'];
    case ExperienceInYear.ThreeYears:
      return ['=', '3'];
    case ExperienceInYear.FourYears:
      return ['=', '4'];
    case ExperienceInYear.FiveYears:
      return ['=', '5'];
    case ExperienceInYear.SixYears:
      return ['=', '6'];
    case ExperienceInYear.SevenYears:
      return ['=', '7'];
    case ExperienceInYear.EightYears:
      return ['=', '8'];
    case ExperienceInYear.NineYears:
      return ['=', '9'];
    case ExperienceInYear.TenYears:
      return ['=', '10'];
    case ExperienceInYear.MoreThanTenYears:
      return ['>', '10'];
  }
}

export function getExperienceFilterForJob(experience: ExperienceInYear) {
  switch (experience) {
    case ExperienceInYear.Fresher:
      return { experienceInYears: { $eq: 0 } };
    case ExperienceInYear.OneYears:
      return { experienceInYears: { $eq: 1 } };
    case ExperienceInYear.TwoYears:
      return { experienceInYears: { $eq: 2 } };
    case ExperienceInYear.ThreeYears:
      return { experienceInYears: { $eq: 3 } };
    case ExperienceInYear.FourYears:
      return { experienceInYears: { $eq: 4 } };
    case ExperienceInYear.FiveYears:
      return { experienceInYears: { $eq: 5 } };
    case ExperienceInYear.SixYears:
      return { experienceInYears: { $eq: 6 } };
    case ExperienceInYear.SevenYears:
      return { experienceInYears: { $eq: 7 } };
    case ExperienceInYear.EightYears:
      return { experienceInYears: { $eq: 8 } };
    case ExperienceInYear.NineYears:
      return { experienceInYears: { $eq: 9 } };
    case ExperienceInYear.TenYears:
      return { experienceInYears: { $eq: 10 } };
    case ExperienceInYear.MoreThanTenYears:
      return { experienceInYears: { $gt: 10 } };
  }
}

export function getExpectedSalaryFilter(expectedSalary: ExpectedSalaryInLpa) {
  switch (expectedSalary) {
    case ExpectedSalaryInLpa.OneLPA:
      return { expectedSalaryInLpa: { $eq: 1 } };
    case ExpectedSalaryInLpa.TwoLPA:
      return { expectedSalaryInLpa: { $eq: 2 } };
    case ExpectedSalaryInLpa.ThreeLPA:
      return { expectedSalaryInLpa: { $eq: 3 } };
    case ExpectedSalaryInLpa.FourLPA:
      return { expectedSalaryInLpa: { $eq: 4 } };
    case ExpectedSalaryInLpa.FiveLPA:
      return { expectedSalaryInLpa: { $eq: 5 } };
    case ExpectedSalaryInLpa.SixLPA:
      return { expectedSalaryInLpa: { $eq: 6 } };
    case ExpectedSalaryInLpa.SevenLPA:
      return { expectedSalaryInLpa: { $eq: 7 } };
    case ExpectedSalaryInLpa.EightLPA:
      return { expectedSalaryInLpa: { $eq: 8 } };
    case ExpectedSalaryInLpa.NineLPA:
      return { expectedSalaryInLpa: { $eq: 9 } };
    case ExpectedSalaryInLpa.TenLPA:
      return { expectedSalaryInLpa: { $eq: 10 } };
    case ExpectedSalaryInLpa.ElevenLPA:
      return { expectedSalaryInLpa: { $eq: 11 } };
    case ExpectedSalaryInLpa.TwelveLPA:
      return { expectedSalaryInLpa: { $eq: 12 } };
    case ExpectedSalaryInLpa.MoreThanTwelveLPA:
      return { expectedSalaryInLpa: { $gt: 12 } };
  }
}
