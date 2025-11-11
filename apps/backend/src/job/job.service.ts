import { JobApplicationStatus } from './../../../contract/enum';
import { Job } from './entities/job.entity';
import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { EntityManager } from '@mikro-orm/postgresql';
import { FilterQuery, LoadStrategy, QueryOrder, wrap } from '@mikro-orm/core';
import { isEmpty, isNil } from 'lodash';
import { Company } from './entities/company.entity';
import {
  City,
  Industry,
  Language,
  Skill,
  Subfunction,
  ToolsAndSoftware,
} from '../user/entities/info.entity';
import { User } from '../user/entities/user.entity';
import { SavedJob } from './entities/savedjobs.entity';
import { JobSeeker } from 'src/user/entities/jobseeker.entity';
import { NestRequestShapes, nestControllerContract } from '@ts-rest/nest';
import { jobContract } from '../../../contract/job/contract';
import { Employer } from 'src/user/entities/employer.entity';
import { JobApplication } from './entities/jobapplication.entity';
import { UserRole } from 'contract/enum';
import { ConfigService } from '@nestjs/config';
import * as dayjs from 'dayjs';
import { bulkUploadJobsForJobSeekerDto } from '../../../contract/job/types';
import {
  BulkUploadJobSeekerJobsErrorType,
  BulkUploadJobSeekerJobsSuccessType,
} from './types';
import { SuccessRO } from 'src/common/success.ro';
import { z } from 'zod';
import * as XLSX from 'xlsx';
import { PaidJobsService } from 'src/paidJobs/paidJobs.service';

export const jobContractController = nestControllerContract(jobContract);
export type JobRequestShapes = NestRequestShapes<typeof jobContractController>;

@Injectable()
export class JobService {
  constructor(
    private em: EntityManager,
    private configService: ConfigService,
    private paidJobService: PaidJobsService,
  ) {}

  getFilterOptions(dto: JobRequestShapes['filter']['query']) {
    const {
      cityIds,
      minSalaryInLpa,
      maxSalaryInLpa,
      employmentType,
      employmentMode,
      searchText,
      industry,
      maxExperienceInYear,
      minExperienceInYear,
      subfunctionIds,
    } = dto;
    let filterOptions: FilterQuery<Job> = {
      adminApprovedTime: { $gte: dayjs().subtract(30, 'days').toDate() },
    };

    if (searchText) {
      filterOptions = {
        ...filterOptions,
        searchVector: {
          $ilike: `%${searchText}%`,
        },
      };
    }
    if (industry) {
      filterOptions.industries = industry;
    }
    if (cityIds) {
      filterOptions.cities = cityIds;
    }
    if (!isNil(minSalaryInLpa)) {
      filterOptions.maxSalaryInLpa = {
        $gte: minSalaryInLpa,
      };
    }

    if (!isNil(maxSalaryInLpa)) {
      filterOptions.minSalaryInLpa = {
        $lte: maxSalaryInLpa,
      };
    }

    if (employmentType) {
      filterOptions.employmentTypes = { $overlap: employmentType };
    }
    if (employmentMode) {
      filterOptions.employmentModes = { $overlap: employmentMode };
    }

    if (minExperienceInYear === 0 && !maxExperienceInYear) {
      filterOptions.minExperienceInYears = { $eq: 0 };
    } else {
      if (!isNil(minExperienceInYear)) {
        filterOptions.maxExperienceInYears = {
          $gte: minExperienceInYear,
        };
      }

      if (!isNil(maxExperienceInYear)) {
        filterOptions.minExperienceInYears = {
          $lte: maxExperienceInYear,
        };
      }
    }
    if (subfunctionIds) {
      filterOptions.subFunctions = subfunctionIds;
    }

    return filterOptions;
  }

  async filter(user: User | null, dto: JobRequestShapes['filter']['query']) {
    const { pageSize, pageNumber } = dto;

    const filterOptions: FilterQuery<Job> = this.getFilterOptions(dto);

    const [jobs, count] = await this.em.findAndCount(
      Job,
      {
        ...filterOptions,
      },
      {
        fields: [
          'id',
          'title',
          'company.name',
          'company.logo',
          'employmentTypes',
          'employmentModes',
          'minSalaryInLpa',
          'maxSalaryInLpa',
          'minExperienceInYears',
          'maxExperienceInYears',
          'adminApprovedTime',
          'cities',
          'industries',
          'subFunctions',
          'isExternalJob',
          'externalLink',
          'externalJobCompanyName',
          'createdAt',
          'companyName',
          'companyLogo',
        ],
        offset: (pageNumber - 1) * pageSize,
        limit: pageSize,
        orderBy: [{ createdAt: QueryOrder.DESC }, { id: QueryOrder.DESC }],
        populate: ['company', 'cities', 'industries', 'subFunctions'],
        strategy: LoadStrategy.SELECT_IN,
      },
    );

    const savedJobs: SavedJob[] = user
      ? await this.em.find(SavedJob, {
          user: user,
          job: { id: { $in: jobs.map((j) => j.id) } },
        })
      : [];

    return { jobs, count, savedJobs };
  }

  async viewJobAsJobseeker(
    user: User | null,
    dto: JobRequestShapes['getJobById']['query'],
  ) {
    const { jobId } = dto;

    const [job, saved, applied] = await Promise.all([
      await this.em.findOne(
        Job,
        { id: jobId },
        {
          populate: ['company', 'cities', 'industries', 'subFunctions'],
        },
      ),
      user
        ? await this.em.findOne(SavedJob, {
            user: user,
            job: { id: jobId },
          })
        : null,
      user
        ? await this.em.findOne(JobApplication, {
            jobSeeker: user,
            job: { id: jobId },
          })
        : null,
    ]);

    if (!job) {
      throw new NotFoundException(
        'This job might have been deleted or removed by the employer',
      );
    }
    const isSaved = saved !== null;
    const isApplied = applied !== null;

    return { job, isSaved, isApplied };
  }

  async getLocations(dto: JobRequestShapes['getSuggestionLocation']['query']) {
    const sText = dto.searchText;
    const locations = await this.em.find(
      City,
      sText ? { name: { $ilike: `%${sText}%` } } : {},
    );

    return locations;
  }

  async getSkills(dto: JobRequestShapes['getSuggestionSkill']['query']) {
    const sText = dto.searchText;
    const skills = await this.em.find(
      Skill,
      sText ? { name: { $ilike: `%${sText}%` } } : {},
    );
    return skills;
  }

  async getIndustries(dto: JobRequestShapes['getSuggestionIndustry']['query']) {
    const sText = dto.searchText;
    const industries = await this.em.find(
      Industry,
      sText ? { name: { $ilike: `%${sText}%` } } : {},
    );
    return industries;
  }

  async getLanguages(dto: JobRequestShapes['getSuggestionLanguages']['query']) {
    const sText = dto.searchText;
    const languages = await this.em.find(
      Language,
      sText ? { name: { $ilike: `%${sText}%` } } : {},
    );
    return languages;
  }

  async getSuggestionToolsAndSoftware(
    dto: JobRequestShapes['getSuggestionToolsAndSoftware']['query'],
  ) {
    const sText = dto.searchText;
    const toolsAndSoftware = await this.em.find(
      ToolsAndSoftware,
      sText ? { name: { $ilike: `%${sText}%` } } : {},
    );
    return toolsAndSoftware;
  }

  async getSubFunctions(
    dto: JobRequestShapes['getSuggestionSubFunction']['query'],
  ) {
    const sText = dto.searchText;
    const subfunctions = await this.em.find(
      Subfunction,
      sText ? { name: { $ilike: `%${sText}%` } } : {},
    );
    return subfunctions;
  }

  async applyJob(user: User, dto: JobRequestShapes['applyJob']['body']) {
    const jobId = dto.jobId;

    const job = await this.em.findOneOrFail(Job, {
      id: jobId,
    });

    const existingJobApplication = await this.em.findOne(JobApplication, {
      jobSeeker: user,
      job: job,
    });

    if (existingJobApplication) {
      throw new BadRequestException('You have already applied for this job');
    }
    if (user.role !== UserRole.jobSeeker || !user.jobSeeker) {
      throw new BadRequestException('You are not a jobSeeker');
    }

    const jobApplication = new JobApplication({
      jobSeeker: user.jobSeeker,
      job: job,
      coverLetter: dto.coverLetter,
      areYouOkayWithTheLocation: dto.areYouOkayWithTheLocation,
    });
    await this.em.persistAndFlush(jobApplication);
  }

  async getAppliedJobs(
    user: User,
    dto: JobRequestShapes['getAppliedJobs']['query'],
  ) {
    const { pageSize, pageNumber } = dto;

    const [appliedJobs, count] = await this.em.findAndCount(
      JobApplication,
      { jobSeeker: user },
      {
        offset: (pageNumber - 1) * pageSize,
        limit: pageSize,
        orderBy: { createdAt: QueryOrder.DESC },
        populate: [
          'job',
          'job.company',
          'job.cities',
          'job.industries',
          'job.subFunctions',
        ],
        filters: false,
      },
    );

    return { appliedJobs, count };
  }

  async saveJob(user: User, dto: JobRequestShapes['saveJob']['body']) {
    const u = await this.em.populate(user, ['jobSeeker']);
    const jobSeeker = u.jobSeeker;

    if (!jobSeeker) {
      throw new BadRequestException(' You are not an jobSeeker');
    }
    const jobId = dto.jobId;
    const job = await this.em.findOneOrFail(Job, {
      id: jobId,
    });
    const savedJob = await this.em.findOne(
      SavedJob,
      {
        user,
        job,
      },
      {
        filters: false,
      },
    );
    if (savedJob) {
      wrap(savedJob).assign({
        isDeleted: false,
      });
    } else {
      const saveJob = new SavedJob({
        user: user,
        job: job,
        isDeleted: false,
      });
      this.em.persist(saveJob);
    }
    await this.em.flush();
  }

  async unsaveJob(user: User, dto: JobRequestShapes['unsaveJob']['body']) {
    const savedJob = await this.em.findOneOrFail(SavedJob, {
      user,
      job: { id: dto.jobId },
    });
    wrap(savedJob).assign({
      isDeleted: true,
    });
    await this.em.flush();
  }

  async fetchSavedJobs(
    user: User,
    dto: JobRequestShapes['fetchSavedJobs']['query'],
  ) {
    const { pageSize, pageNumber } = dto;

    const [savedJobs, count] = await this.em.findAndCount(
      SavedJob,
      {
        user: user,
        isDeleted: false,
      },
      {
        offset: (pageNumber - 1) * pageSize,
        limit: pageSize,
        orderBy: { createdAt: QueryOrder.DESC },
        populate: [
          'job',
          'job.company',
          'job.cities',
          'job.industries',
          'job.subFunctions',
        ],
        filters: false,
      },
    );

    return { savedJobs, count };
  }

  async fetchJobsByEmployer(
    user: User,
    dto: JobRequestShapes['fetchJobsByEmployer']['query'],
  ) {
    const { pageSize, pageNumber } = dto;
    const employer = user.employer;

    if (!employer) {
      throw new BadRequestException('You are not an employer');
    }

    const [jobs, count] = await this.em.findAndCount(
      Job,
      {
        createdByEmployer: employer,
      },
      {
        offset: (pageNumber - 1) * pageSize,
        limit: pageSize,
        orderBy: [{ createdAt: QueryOrder.DESC }, { id: QueryOrder.DESC }],
        populate: [
          'cities',
          'company',
          'industries',
          'subFunctions',
          'jobBoosts',
          'jobBoosts.subscription',
        ],
      },
    );
    return { jobs, count };
  }

  async deleteJobAsEmployer(
    dto: JobRequestShapes['deleteJobAsEmployer']['body'],
    user: User,
  ) {
    const id = dto.jobId;
    const job = await this.em.findOneOrFail(Job, {
      id,
      createdByEmployer: user,
    });
    wrap(job).assign({
      isDeleted: true,
    });

    await this.em.flush();
  }

  getSearchVector(job: Job) {
    let searchVector = '';

    searchVector += job.title + ' ';
    searchVector += job.companyName + ' ';
    searchVector += job.cities.map((c) => c.name).join(',') + ' ';
    searchVector += job.industries.map((i) => i.name).join(',') + ' ';
    searchVector += job.subFunctions.map((sf) => sf.name).join(',') + ' ';
    searchVector += job.employmentModes.join(',') + ' ';
    searchVector += job.employmentTypes.join(',') + ' ';
    searchVector +=
      (job.minSalaryInLpa ? `${job.minSalaryInLpa} LPA` : '') + ' ';
    searchVector +=
      (job.maxSalaryInLpa ? `${job.maxSalaryInLpa} LPA` : '') + ' ';
    searchVector +=
      (job.minExperienceInYears
        ? `${job.minExperienceInYears} years of experience`
        : '') + ' ';
    return searchVector;
  }

  // @Schedule('fillsearchVectors', CronExpressions.everyMinute)
  // async setSearchVectors() {
  //   const jobs = await this.em.find(Job, {}, { limit: 100, populate: true });

  //   jobs.forEach((job) => {
  //     wrap(job).assign({
  //       searchVector: this.getSearchVector(job),
  //     });
  //   });

  //   await this.em.flush();
  // }

  async createJobForJobSeeker(
    user: User,
    dto: JobRequestShapes['createJobForJobSeeker']['body'],
  ) {
    const [company, cities, industries, subfunctions, employer] =
      await Promise.all([
        this.em.findOneOrFail(Company, { id: dto.companyId }),
        this.em.find(City, { id: { $in: dto.cityIds } }),
        this.em.find(Industry, { id: { $in: dto.industryIds } }),
        this.em.find(Subfunction, { id: { $in: dto.subfunctionIds } }),
        this.em.findOneOrFail(Employer, { user: user.id }),
      ]);

    const job = new Job({
      title: dto.title,
      company: company,
      cities: cities,
      industries: industries,
      subFunctions: subfunctions,
      description: dto.description ? JSON.parse(dto.description) : '',
      minSalaryInLpa: dto.minSalaryInLpa || null,
      maxSalaryInLpa: dto.maxSalaryInLpa || null,
      employmentTypes: dto.employmentTypes,
      employmentModes: dto.employmentModes,
      minExperienceInYears: dto.minExperienceInYears,
      maxExperienceInYears: dto.maxExperienceInYears,
      createdByEmployer: employer,
      isPosted: dto.isPosted,
      searchVector: '',
    });

    if (dto.isPosted) {
      wrap(job).assign({
        postedTime: new Date(),
      });
    }

    wrap(job).assign({
      searchVector: this.getSearchVector(job),
    });

    if (dto.isPremium && dto.subscriptionId) {
      await this.paidJobService.createJobBoost(dto.subscriptionId, {
        employer,
        job,
      });
    }

    await this.em.persistAndFlush(job);
  }

  async getCompanyName(
    dto: JobRequestShapes['getCompanyNameSuggestions']['query'],
  ) {
    const sText = dto.searchText;
    const companies = await this.em.find(
      Company,
      sText ? { name: { $ilike: `%${sText}%` } } : {},
    );

    return companies;
  }

  async changeJobSeekerApplicationStatus(
    user: User,
    dto: JobRequestShapes['changeJobSeekerApplicationStatus']['body'],
  ) {
    const jobSeeker = await this.em.findOne(JobSeeker, {
      user: { id: dto.applicantUserId },
    });
    const application = await this.em.findOneOrFail(
      JobApplication,
      {
        jobSeeker: jobSeeker,
        job: { id: dto.jobId, createdByEmployer: { user: user.id } },
      },
      { populate: ['jobSeeker', 'jobSeeker.user'] },
    );

    wrap(application).assign({
      status: dto.changedStatus,
      statusChangedTime: new Date(),
    });

    await this.em.flush();
  }

  async viewJobSeekersProfile(
    dto: JobRequestShapes['viewJobSeekersProfile']['query'],
  ) {
    const { userId } = dto;
    const user = await this.em.findOneOrFail(
      User,
      { id: userId },
      {
        populate: [
          'jobSeeker',
          'jobSeeker.preferredLocations',
          'jobSeeker.languages',
        ],
      },
    );

    return user;
  }

  getFilterOptionsForManageJobSeekerJobs(
    dto: JobRequestShapes['manageJobApplicationsOfJobseekers']['query'],
  ) {
    const {
      noticePeriod,
      locationIds,
      minExperienceInYears,
      maxExperienceInYears,
      minSalaryInLpa,
      maxSalaryInLpa,
      skillIds,
      languageIds,
    } = dto;

    let filterOptions: FilterQuery<JobSeeker> = {};

    if (noticePeriod) {
      filterOptions.noticePeriod = { $in: noticePeriod };
    }

    if (locationIds) {
      filterOptions.city = locationIds;
    }
    if (skillIds) {
      filterOptions.skills = skillIds;
    }
    if (languageIds) {
      filterOptions.languages = languageIds;
    }

    if (!isNil(minSalaryInLpa) && !isNil(maxSalaryInLpa)) {
      filterOptions = {
        ...filterOptions,
        $and: [
          { expectedSalaryInLpa: { $gte: minSalaryInLpa } },
          { expectedSalaryInLpa: { $lte: maxSalaryInLpa } },
        ],
      };
    } else {
      if (!isNil(minSalaryInLpa)) {
        filterOptions.expectedSalaryInLpa = { $gte: minSalaryInLpa };
      }

      if (!isNil(maxSalaryInLpa)) {
        filterOptions.expectedSalaryInLpa = { $lte: maxSalaryInLpa };
      }
    }

    if (!isNil(minExperienceInYears) && !isNil(maxExperienceInYears)) {
      filterOptions.experienceInYear = {
        $gte: minExperienceInYears,
        $lte: maxExperienceInYears,
      };
    } else {
      if (!isNil(minExperienceInYears)) {
        filterOptions.experienceInYear = { $gte: minExperienceInYears };
      }

      if (!isNil(maxExperienceInYears)) {
        filterOptions.experienceInYear = { $lte: maxExperienceInYears };
      }
    }

    if (dto.jobSeekerName) {
      filterOptions = {
        ...filterOptions,

        $or: [
          {
            user: {
              fullName: { $ilike: `%${dto.jobSeekerName}%` },
            },
          },
        ],
      };
    }

    return filterOptions;
  }

  async manageJobApplicationsOfJobseekers(
    user: User,
    dto: JobRequestShapes['manageJobApplicationsOfJobseekers']['query'],
  ) {
    const { pageSize, pageNumber, status, jobId } = dto;

    const jobSeekerFilterOptions: FilterQuery<JobSeeker> =
      this.getFilterOptionsForManageJobSeekerJobs(dto);

    const [jobApplications, count] = await this.em.findAndCount(
      JobApplication,

      {
        status: status,
        job: {
          id: jobId,
          createdByEmployer: {
            user: user,
          },
        },
        jobSeeker: jobSeekerFilterOptions,
      },
      {
        offset: (pageNumber - 1) * pageSize,
        limit: pageSize,
        orderBy: { createdAt: QueryOrder.DESC },
        populate: [
          'jobSeeker',
          'jobSeeker.city',
          'job',
          'job.createdByEmployer',
          'jobSeeker.user',
        ],
      },
    );

    const knex = this.em.getKnex();

    const lastActiveData: {
      user_id: number;
      max: string;
    }[] = await knex
      .select('user_id')
      .max('created_at')
      .from('analytic_event')
      .whereIn(
        'user_id',
        jobApplications.map((j) => j.jobSeeker.user.id),
      )
      .groupBy('user_id');

    const { pendingCount, shortlistedCount, rejectedCount } =
      await this.getJobseekerJobApplicationCountByStatus(
        user,
        dto,
        jobSeekerFilterOptions,
      );

    return {
      jobApplications,
      count,
      lastActiveData,
      pendingCount,
      shortlistedCount,
      rejectedCount,
    };
  }

  async getJobseekerJobApplicationCountByStatus(
    user: User,
    dto: JobRequestShapes['manageJobApplicationsOfJobseekers']['query'],
    jobSeekerFilterOptions: FilterQuery<JobSeeker>,
  ) {
    const { jobId } = dto;

    const [pendingCount, shortlistedCount, rejectedCount] = await Promise.all([
      this.em.count(JobApplication, {
        status: JobApplicationStatus.Pending,
        job: {
          id: jobId,
          createdByEmployer: {
            user: user,
          },
        },
        jobSeeker: jobSeekerFilterOptions,
      }),
      this.em.count(
        JobApplication,

        {
          status: JobApplicationStatus.Shortlisted,
          job: {
            id: jobId,
            createdByEmployer: {
              user: user,
            },
          },
          jobSeeker: jobSeekerFilterOptions,
        },
      ),
      this.em.count(
        JobApplication,

        {
          status: JobApplicationStatus.Rejected,
          job: {
            id: jobId,
            createdByEmployer: {
              user: user,
            },
          },
          jobSeeker: jobSeekerFilterOptions,
        },
      ),
    ]);

    return { pendingCount, shortlistedCount, rejectedCount };
  }

  async changeIsPost(
    user: User,
    dto: JobRequestShapes['changeIsPost']['body'],
  ) {
    const jobId = dto.jobId;
    const u = await this.em.populate(user, ['employer']);
    const employer = u.employer;

    if (!employer) {
      throw new BadRequestException('You are not an employer');
    }

    const job = await this.em.findOneOrFail(Job, {
      id: jobId,
    });
    wrap(job).assign({
      isPosted: true,
      postedTime: new Date(),
    });
    await this.em.flush();
  }

  frontEndUrl = this.configService.get<string>('frontEndUrl');

  async getSuggestionJobTitleInManageApplicantsJobseeker(
    user: User,
    dto: JobRequestShapes['getSuggestionJobTitleInManageApplicantsJobseeker']['query'],
  ) {
    const u = await this.em.populate(user, ['employer']);
    const employer = u.employer;
    const searchText = dto.searchText;

    const jobs = await this.em.find(Job, {
      title: searchText ? { $ilike: `%${searchText}%` } : {},
      createdByEmployer: employer,
    });

    return { jobs };
  }

  async bulkUploadJobsForJobSeeker(file: Express.Multer.File) {
    const xlFile = XLSX.read(file.buffer);

    const json: Record<string, unknown>[] = XLSX.utils.sheet_to_json(
      xlFile.Sheets[Object.keys(xlFile.Sheets)[0]],
      { raw: false },
    );
    const validatedObjs = json.map(
      (q: Record<string, unknown>, index: number) => {
        return this.validateJobSeekerJobDatas(q, index);
      },
    );

    const errorObjs = validatedObjs.filter(
      (validatedData): validatedData is BulkUploadJobSeekerJobsErrorType => {
        return validatedData.kind === 'error';
      },
    );

    if (errorObjs.length > 0) {
      return { errors: errorObjs.map((o) => o.errors).flat() };
    }

    const dtos = validatedObjs
      .filter(
        (v): v is BulkUploadJobSeekerJobsSuccessType => v.kind === 'success',
      )
      .map((v) => v.data);

    const [cities, industries, subfunctions] = await Promise.all([
      this.em.find(City, {
        name: { $in: [...new Set(dtos.map((d) => d.location).flat())] },
      }),
      this.em.find(Industry, {
        name: { $in: [...new Set(dtos.map((d) => d.industries).flat())] },
      }),
      this.em.find(Subfunction, {
        name: { $in: [...new Set(dtos.map((d) => d.subfunctions).flat())] },
      }),
    ]);

    const dtoDataWithAdditionalData = dtos.map((d) => {
      const c = cities.filter((c) => d.location.includes(c.name));
      const i = industries.filter((c) => d.industries.includes(c.name));
      const sf = subfunctions.filter((c) => d.subfunctions.includes(c.name));
      return {
        data: d,
        cities: c,
        industries: i,
        subfunctions: sf,
      };
    });

    const companyValidationErrors = dtoDataWithAdditionalData
      .map((d, index) => {
        const errors = [];

        if (!d.data.externalLink) {
          errors.push(`externalLink is not found at line no : ${index + 1}`);
        }
        if (d.cities.length !== d.data.location.length) {
          errors.push(`Some of the cities are not found at line no : ${
            index + 1
          }.
           The cities are ${d.data.location.filter(
             (c) => !d.cities.map((c) => c.name).includes(c),
           )}`);
        }
        if (d.industries.length !== d.data.industries.length) {
          errors.push(`Some of the industries are not found at line no : ${
            index + 1
          }.
           The industries are ${d.data.industries.filter(
             (c) => !d.industries.map((c) => c.name).includes(c),
           )}`);
        }
        if (d.subfunctions.length !== d.data.subfunctions.length) {
          errors.push(`Some of the subfunctions are not found at line no : ${
            index + 1
          }.
           The subfunctions are ${d.data.subfunctions.filter(
             (c) => !d.subfunctions.map((c) => c.name).includes(c),
           )}`);
        }
        return errors;
      })
      .filter((e) => e.length > 0)
      .flat();

    if (!isEmpty(companyValidationErrors)) {
      return {
        errors: companyValidationErrors,
      };
    }
    const updatedData = dtoDataWithAdditionalData.map(
      ({ cities, industries, subfunctions, ...rest }) => rest,
    );

    const jobseekerJobs = updatedData.map((eachData) => {
      const d = eachData.data;
      let job = new Job({
        ...d,
        title: d.jobTitle,
        externalLink: d.externalLink ? d.externalLink : null,
        externalJobCompanyName: d.companyName,
        description: null,
        minSalaryInLpa: d.minSalaryInLpa || null,
        maxSalaryInLpa: d.maxSalaryInLpa || null,
        cities: cities.filter((c) => d.location.includes(c.name)),
        industries: industries.filter((c) => d.industries.includes(c.name)),
        subFunctions: subfunctions.filter((c) =>
          d.subfunctions.includes(c.name),
        ),
        company: null,
        createdByEmployer: null,
        isPosted: true,
        isDeleted: false,
        searchVector: '',
        maxExperienceInYears: d.maxExperienceInYears,
        minExperienceInYears: d.minExperienceInYears,
        employmentTypes: d.typeOfEmployment,
        employmentModes: d.workSchedule,
      });
      job.isExternalJob = true;
      job.searchVector = this.getSearchVector(job);
      job.isAdminApproved = true;
      job.adminApprovedTime = new Date();
      job.postedTime = new Date();
      return job;
    });

    this.em.persist(jobseekerJobs);
    await this.em.flush();

    return new SuccessRO();
  }

  validateJobSeekerJobDatas(
    data: Record<string, unknown>,
    index: number,
  ): BulkUploadJobSeekerJobsSuccessType | BulkUploadJobSeekerJobsErrorType {
    const lineNumber = index + 1;
    try {
      // business logic errors
      const errors: string[] = [];

      if (isNil(data.jobType)) {
        return {
          kind: 'error',
          data,
          errors: [
            `No job type found for the given data at line no : ${lineNumber}`,
          ],
        };
      }
      if (UserRole.jobSeeker !== data.jobType) {
        return {
          kind: 'error',
          data,
          errors: [
            `Expected data of format ${UserRole.jobSeeker} but received ${data.jobType}  at line no : ${lineNumber}`,
          ],
        };
      }
      const transformedData = bulkUploadJobsForJobSeekerDto.parse(data);

      if (errors.length > 0) {
        return {
          kind: 'error',
          data: transformedData,
          errors,
        };
      }
      if (transformedData.maxSalaryInLpa <= transformedData.minSalaryInLpa) {
        return {
          kind: 'error',
          data: transformedData,
          errors: [
            `maxSalaryInLpa should be greater than minSalaryInLpa at line no : ${lineNumber}`,
          ],
        };
      }
      return { kind: 'success', data: transformedData, errors };
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          kind: 'error',
          data,
          errors: error.errors.map(
            (err) =>
              `${err.path.join('.')}: ${
                err.message
              }  at line no : ${lineNumber}`,
          ),
        };
      }
      throw error;
    }
  }

  async deleteJobseekerJobAsAdmin(
    dto: JobRequestShapes['deleteJobseekerJobAsAdmin']['body'],
  ) {
    const id = dto.jobId;
    const job = await this.em.findOneOrFail(Job, {
      id,
    });
    wrap(job).assign({
      isDeleted: true,
    });
    await this.em.flush();
  }
}
