import {
  BadRequestException,
  Controller,
  UploadedFile,
  UseInterceptors,
} from '@nestjs/common';
import { JobService } from './job.service';
import {
  EmployerOnlyAuth,
  getUserFromToken,
  OptionalUserFromToken,
  OptionalAuth,
  JobSeekerOnlyAuth,
  AdminOnlyAuth,
} from '../common/decorators/user.decorator';

import { User } from '../user/entities/user.entity';
import {
  NestControllerInterface,
  NestRequestShapes,
  TsRest,
  TsRestRequest,
  nestControllerContract,
} from '@ts-rest/nest';
import { contract } from 'contract';
import { UserRole } from 'contract/enum';
import { FileInterceptor } from '@nestjs/platform-express';
import {} from '@ts-rest/core'; // TO prevent zod error while building
import { calculateBoostedDaysLeft, isJobBoosted } from 'src/utils';
import { LoadedUser } from 'src/user/user.service';

export const jobContractController = nestControllerContract(contract.job);
export type JobRequestShapes = NestRequestShapes<typeof jobContractController>;

@Controller()
export class JobController
  implements NestControllerInterface<typeof jobContractController>
{
  constructor(private readonly jobService: JobService) {}

  @OptionalAuth()
  @TsRest(jobContractController.filter)
  async filter(
    @getUserFromToken() user: User,
    @TsRestRequest() { query }: JobRequestShapes['filter'],
  ) {
    const { jobs, count, savedJobs } = await this.jobService.filter(
      user,
      query,
    );
    return {
      status: 200 as const,
      body: {
        currentPageSize: query.pageSize,
        currentPageNumber: query.pageNumber,
        results: jobs.map((j) => ({
          id: j.id,
          title: j.title,
          types: j.employmentTypes,
          modes: j.employmentModes,
          minExp: j.minExperienceInYears,
          maxExp: j.maxExperienceInYears,
          companyName: j.companyName,
          companyLogo: j.companyLogo,
          minCtc: j.minSalaryInLpa,
          maxCtc: j.maxSalaryInLpa,
          cities: j.cities.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          industries: j.industries.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          subFunctions: j.subFunctions.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          createdAt: j.createdAt,
          isExternalJob: j.isExternalJob,
          externalLink: j.externalLink,
          isSaved: savedJobs.some((s) => s.job.id === j.id),
          adminApprovedTime: j.adminApprovedTime,
        })),
        totalItems: count,
        totalPages: Math.ceil(count / query.pageSize),
      },
    };
  }

  @OptionalAuth()
  @TsRest(jobContractController.getJobById)
  async getJobById(
    @TsRestRequest() { query }: JobRequestShapes['getJobById'],
    @OptionalUserFromToken() user: User | null,
  ) {
    const { job, isSaved, isApplied } =
      await this.jobService.viewJobAsJobseeker(user, query);
    return {
      status: 200 as const,
      body: {
        id: job.id,
        designation: job.title,
        companyName: job.companyName,
        companyLogo: job.companyLogo,
        locations: job.cities.$.getItems().map((c) => ({
          id: c.id,
          name: c.name,
        })),
        industries: job.industries.$.getItems().map((c) => ({
          id: c.id,
          name: c.name,
        })),
        subFunctions: job.subFunctions.$.getItems().map((c) => ({
          id: c.id,
          name: c.name,
        })),
        employmentModes: job.employmentModes,
        employmentTypes: job.employmentTypes,
        description: job.description,
        minSalaryInlpa: job.minSalaryInLpa,
        maxSalaryInLpa: job.maxSalaryInLpa,
        minExperienceInYears: job.minExperienceInYears,
        maxExperienceInYears: job.maxExperienceInYears,
        createdAt: job.createdAt,
        isExternalJob: job.isExternalJob,
        externalLink: job.externalLink,
        isSaved: isSaved,
        isApplied: isApplied,
        adminApprovedTime: job.adminApprovedTime,
      },
    };
  }

  @TsRest(jobContractController.getSuggestionLocation)
  async getSuggestionLocation(
    @TsRestRequest() { query }: JobRequestShapes['getSuggestionLocation'],
  ) {
    const locations = await this.jobService.getLocations(query);
    return {
      status: 200 as const,
      body: locations.map((l) => ({ id: l.id, name: l.name })),
    };
  }

  @TsRest(jobContractController.getSuggestionSkill)
  async getSuggestionSkill(
    @TsRestRequest() { query }: JobRequestShapes['getSuggestionSkill'],
  ) {
    const skills = await this.jobService.getSkills(query);
    return {
      status: 200 as const,
      body: skills.map((s) => ({ id: s.id, name: s.name })),
    };
  }

  @TsRest(jobContractController.getSuggestionIndustry)
  async getSuggestionIndustry(
    @TsRestRequest() { query }: JobRequestShapes['getSuggestionIndustry'],
  ) {
    const industries = await this.jobService.getIndustries(query);
    return {
      status: 200 as const,
      body: industries.map((i) => ({ id: i.id, name: i.name })),
    };
  }

  @TsRest(jobContractController.getSuggestionSubFunction)
  async getSuggestionSubFunction(
    @TsRestRequest() { query }: JobRequestShapes['getSuggestionSubFunction'],
  ) {
    const subFunctions = await this.jobService.getSubFunctions(query);
    return {
      status: 200 as const,
      body: subFunctions.map((sf) => ({ id: sf.id, name: sf.name })),
    };
  }

  @TsRest(jobContractController.getSuggestionLanguages)
  async getSuggestionLanguages(
    @TsRestRequest() { query }: JobRequestShapes['getSuggestionLanguages'],
  ) {
    const languages = await this.jobService.getLanguages(query);
    return {
      status: 200 as const,
      body: languages.map((language) => ({
        id: language.id,
        name: language.name,
      })),
    };
  }
  @TsRest(jobContractController.getSuggestionToolsAndSoftware)
  async getSuggestionToolsAndSoftware(
    @TsRestRequest()
    { query }: JobRequestShapes['getSuggestionToolsAndSoftware'],
  ) {
    const toolsAndSoftware =
      await this.jobService.getSuggestionToolsAndSoftware(query);
    return {
      status: 200 as const,
      body: toolsAndSoftware.map((toolAndSoftware) => ({
        id: toolAndSoftware.id,
        name: toolAndSoftware.name,
      })),
    };
  }

  @JobSeekerOnlyAuth()
  @TsRest(jobContractController.applyJob)
  async applyJob(
    @getUserFromToken() user: User,
    @TsRestRequest() { body }: JobRequestShapes['applyJob'],
  ) {
    await this.jobService.applyJob(user, body);
    return {
      status: 200 as const,
      body: {
        isSuccess: true,
        message: 'Job Applied Successfully',
      },
    };
  }

  @JobSeekerOnlyAuth()
  @TsRest(jobContractController.getAppliedJobs)
  async getAppliedJobs(
    @getUserFromToken() user: User,
    @TsRestRequest() { query }: JobRequestShapes['getAppliedJobs'],
  ) {
    const { appliedJobs, count } = await this.jobService.getAppliedJobs(
      user,
      query,
    );
    return {
      status: 200 as const,
      body: {
        currentPageSize: query.pageSize,
        currentPageNumber: query.pageNumber,
        results: appliedJobs.map((j) => ({
          id: j.job.id,
          title: j.job.title,
          companyLogo: j.job.companyLogo,
          employmentTypes: j.job.employmentTypes,
          employmentModes: j.job.employmentModes,
          minExp: j.job.minExperienceInYears,
          maxExp: j.job.maxExperienceInYears,
          companyName: j.job.companyName,
          minCtc: j.job.minSalaryInLpa,
          maxCtc: j.job.maxSalaryInLpa,
          cities: j.job.cities.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          description: j.job.description,
          industries: j.job.industries.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          subFunctions: j.job.subFunctions.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          isExternalJob: j.job.isExternalJob,
          externalLink: j.job.externalLink,
          jobAppliedTime: j.createdAt,
          status: j.status,
          statusChangedTime: j.statusChangedTime,
        })),
        totalItems: count,
        totalPages: Math.ceil(count / query.pageSize),
      },
    };
  }

  @JobSeekerOnlyAuth()
  @TsRest(jobContractController.saveJob)
  async saveJob(
    @getUserFromToken() user: User,
    @TsRestRequest() { body }: JobRequestShapes['saveJob'],
  ) {
    await this.jobService.saveJob(user, body);
    return {
      status: 200 as const,
      body: {
        isSuccess: true,
        message: 'Job saved Successfully',
      },
    };
  }

  @JobSeekerOnlyAuth()
  @TsRest(jobContractController.unsaveJob)
  async unsaveJob(
    @getUserFromToken() user: User,
    @TsRestRequest() { body }: JobRequestShapes['unsaveJob'],
  ) {
    await this.jobService.unsaveJob(user, body);
    return {
      status: 200 as const,
      body: {
        isSuccess: true,
        message: ' Saved Job Deleted Successfully',
      },
    };
  }

  @JobSeekerOnlyAuth()
  @TsRest(jobContractController.fetchSavedJobs)
  async fetchSavedJobs(
    @getUserFromToken() user: User,
    @TsRestRequest() { query }: JobRequestShapes['fetchSavedJobs'],
  ) {
    const { savedJobs, count } = await this.jobService.fetchSavedJobs(
      user,
      query,
    );
    return {
      status: 200 as const,
      body: {
        currentPageSize: query.pageSize,
        currentPageNumber: query.pageNumber,
        results: savedJobs.map((j) => ({
          id: j.job.id,
          title: j.job.title,
          companyLogo: j.job.companyLogo,
          employmentTypes: j.job.employmentTypes,
          employmentModes: j.job.employmentModes,
          minExp: j.job.minExperienceInYears,
          maxExp: j.job.maxExperienceInYears,
          companyName: j.job.companyName,
          minCtc: j.job.minSalaryInLpa,
          maxCtc: j.job.maxSalaryInLpa,
          cities: j.job.cities.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          description: j.job.description,
          industries: j.job.industries.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          subFunctions: j.job.subFunctions.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          jobSavedTime: j.createdAt,
          isExternalJob: j.job.isExternalJob,
          externalLink: j.job.externalLink,
        })),
        totalItems: count,
        totalPages: Math.ceil(count / query.pageSize),
      },
    };
  }

  @EmployerOnlyAuth()
  @TsRest(jobContractController.fetchJobsByEmployer)
  async fetchJobsByEmployer(
    @getUserFromToken() user: LoadedUser,
    @TsRestRequest() { query }: JobRequestShapes['fetchJobsByEmployer'],
  ) {
    if (!user.employer) {
      throw new BadRequestException('You are not an employer');
    }
    const { jobs, count } = await this.jobService.fetchJobsByEmployer(
      user,
      query,
    );

    return {
      status: 200 as const,
      body: {
        currentPageSize: query.pageSize,
        currentPageNumber: query.pageNumber,
        results: jobs.map((j) => ({
          id: j.id,
          title: j.title,
          employmentTypes: j.employmentTypes,
          employmentModes: j.employmentModes,
          minExp: j.minExperienceInYears,
          maxExp: j.maxExperienceInYears,
          companyName: j.companyName,
          companyLogo: j.companyLogo,
          minCtc: j.minSalaryInLpa,
          maxCtc: j.maxSalaryInLpa,
          cities: j.cities.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          description: j.description,
          industries: j.industries.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          subFunctions: j.subFunctions.$.getItems().map((c) => ({
            id: c.id,
            name: c.name,
          })),
          createdAt: j.createdAt,
          isExternalJob: j.isExternalJob,
          externalLink: j.externalLink,
          isPosted: j.isPosted,
          isAdminApproved: j.isAdminApproved,
          adminApprovedTime: j.adminApprovedTime,
          postedTime: j.postedTime,
          isBoosted: isJobBoosted(j.activeJobBoosts),
          boostedDaysLeft: calculateBoostedDaysLeft(j.activeJobBoosts),
        })),
        totalItems: count,
        totalPages: Math.ceil(count / query.pageSize),
      },
    };
  }

  @EmployerOnlyAuth()
  @TsRest(jobContractController.createJobForJobSeeker)
  async createJobForJobSeeker(
    @getUserFromToken() user: User,
    @TsRestRequest() { body }: JobRequestShapes['createJobForJobSeeker'],
  ) {
    await this.jobService.createJobForJobSeeker(user, body);
    return {
      status: 200 as const,
      body: {
        isSuccess: true,
        message: 'Job created Successfully',
      },
    };
  }

  @TsRest(jobContractController.getCompanyNameSuggestions)
  async getCompanyNameSuggestions(
    @TsRestRequest() { query }: JobRequestShapes['getCompanyNameSuggestions'],
  ) {
    const companies = await this.jobService.getCompanyName(query);
    return {
      status: 200 as const,
      body: companies.map((l) => ({ id: l.id, name: l.name })),
    };
  }

  @EmployerOnlyAuth()
  @TsRest(jobContractController.viewJobSeekersProfile)
  async viewJobSeekersProfile(
    @TsRestRequest() { query }: JobRequestShapes['viewJobSeekersProfile'],
  ) {
    const user = await this.jobService.viewJobSeekersProfile(query);
    if (user.role !== UserRole.jobSeeker || !user.jobSeeker) {
      throw new BadRequestException('He/she is not a jobSeeker');
    }
    return {
      status: 200 as const,
      body: {
        userId: user.id,
        firstName: user.firstName,
        lastName: user.lastName,
        email: user.email,
        phone: user.phone,
        role: user.role,
        picture: user.picture,
        profileSummary: user.jobSeeker.profileSummary,
        headline: user.jobSeeker.headline,
        expectedSalaryInLpa: user.jobSeeker.expectedSalaryInLpa,
        resume: user.jobSeeker.resume,
        videoResume: user.jobSeeker.videoResume,
        preferredLocations: user.jobSeeker.preferredLocations.$.getItems().map(
          (pl) => ({ id: pl.id, name: pl.name }),
        ),
        languages: user.jobSeeker.languages.$.getItems().map((l) => ({
          id: l.id,
          name: l.name,
        })),
        experienceInYear: user.jobSeeker.experienceInYear,
        noticePeriod: user.jobSeeker.noticePeriod,
        workSchedule: user.jobSeeker.workSchedule,
      },
    };
  }

  @EmployerOnlyAuth()
  @TsRest(jobContractController.deleteJobAsEmployer)
  async deleteJobAsEmployer(
    @TsRestRequest() { body }: JobRequestShapes['deleteJobAsEmployer'],
    @getUserFromToken() user: User,
  ) {
    await this.jobService.deleteJobAsEmployer(body, user);
    return {
      status: 200 as const,
      body: {
        isSuccess: true,
        message: 'Job Deleted Successfully',
      },
    };
  }

  @EmployerOnlyAuth()
  @TsRest(jobContractController.changeJobSeekerApplicationStatus)
  async changeJobSeekerApplicationStatus(
    @getUserFromToken() user: User,
    @TsRestRequest()
    { body }: JobRequestShapes['changeJobSeekerApplicationStatus'],
  ) {
    await this.jobService.changeJobSeekerApplicationStatus(user, body);
    return {
      status: 200 as const,
      body: {
        isSuccess: true,
        message: 'Status Changed  Successfully',
      },
    };
  }

  @EmployerOnlyAuth()
  @TsRest(jobContractController.changeIsPost)
  async changeIsPost(
    @getUserFromToken() user: User,
    @TsRestRequest() { body }: JobRequestShapes['changeIsPost'],
  ) {
    await this.jobService.changeIsPost(user, body);
    return {
      status: 200 as const,
      body: {
        isSuccess: true,
        message: 'Job Posted Successfully!!!!',
      },
    };
  }

  @EmployerOnlyAuth()
  @TsRest(jobContractController.manageJobApplicationsOfJobseekers)
  async manageJobApplicationsOfJobseekers(
    @getUserFromToken() user: User,
    @TsRestRequest()
    { query }: JobRequestShapes['manageJobApplicationsOfJobseekers'],
  ) {
    const {
      jobApplications,
      count,
      lastActiveData,
      pendingCount,
      shortlistedCount,
      rejectedCount,
    } = await this.jobService.manageJobApplicationsOfJobseekers(user, query);

    return {
      status: 200 as const,
      body: {
        currentPageSize: query.pageSize,
        currentPageNumber: query.pageNumber,
        results: jobApplications.map((ja) => ({
          jobApplicationId: ja.id,
          jobSeekerId: ja.jobSeeker.user.id,
          jobSeekerProfileImage: ja.jobSeeker.user.picture,
          jobSeekerDescription: ja.jobSeeker.profileSummary,
          jobId: ja.job.id,
          firstName: ja.jobSeeker.user.firstName,
          lastName: ja.jobSeeker.user.lastName,
          experience: ja.jobSeeker.experienceInYear,
          location: ja.jobSeeker.city.name,
          expectedSalaryInLpa: ja.jobSeeker.expectedSalaryInLpa,
          noticePeriod: ja.jobSeeker.noticePeriod,
          resume: ja.jobSeeker.resume,
          videoResume: ja.jobSeeker.videoResume,
          lastLoginTime:
            lastActiveData.find((lad) => lad.user_id == ja.jobSeeker.user.id)
              ?.max ?? null,
          status: ja.status,
          adminApprovedTime: ja.job.adminApprovedTime,
          coverLetter: ja.coverLetter,
        })),
        pendingCount: pendingCount,
        shortlistedCount: shortlistedCount,
        rejectedCount: rejectedCount,
        totalPages: Math.ceil(count / query.pageSize),
      },
    };
  }

  @EmployerOnlyAuth()
  @TsRest(
    jobContractController.getSuggestionJobTitleInManageApplicantsJobseeker,
  )
  async getSuggestionJobTitleInManageApplicantsJobseeker(
    @getUserFromToken() user: User,
    @TsRestRequest()
    {
      query,
    }: JobRequestShapes['getSuggestionJobTitleInManageApplicantsJobseeker'],
  ) {
    const { jobs } =
      await this.jobService.getSuggestionJobTitleInManageApplicantsJobseeker(
        user,
        query,
      );
    return {
      status: 200 as const,
      body: jobs.map((j) => ({
        jobId: j.id,
        jobTitle: j.title,
        adminApprovedTime: j.adminApprovedTime,
      })),
    };
  }

  @AdminOnlyAuth()
  @UseInterceptors(FileInterceptor('file'))
  @TsRest(jobContractController.bulkUploadJobsForJobSeeker)
  async bulkUploadJobsForJobSeeker(@UploadedFile() file: Express.Multer.File) {
    const data = await this.jobService.bulkUploadJobsForJobSeeker(file);
    if ('errors' in data) {
      return {
        status: 200 as const,
        body: { ...data, kind: 'error' as const },
      };
    }
    return {
      status: 200 as const,
      body: { ...data, kind: 'success' as const },
    };
  }

  @AdminOnlyAuth()
  @TsRest(jobContractController.deleteJobseekerJobAsAdmin)
  async deleteJobseekerJobAsAdmin(
    @TsRestRequest() { body }: JobRequestShapes['deleteJobseekerJobAsAdmin'],
  ) {
    await this.jobService.deleteJobseekerJobAsAdmin(body);
    return {
      status: 200 as const,
      body: {
        isSuccess: true,
        message: 'Job Deleted Successfully',
      },
    };
  }
}
