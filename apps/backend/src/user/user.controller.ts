import { BadRequestException, Controller } from '@nestjs/common';
import { UserService } from './user.service';
import {
  nestControllerContract,
  NestControllerInterface,
  NestRequestShapes,
  TsRest,
  TsRestRequest,
} from '@ts-rest/nest';
import {
  Auth,
  EmployerOnlyAuth,
  getUserFromToken,
  JobSeekerOnlyAuth,
} from '../common/decorators/user.decorator';
import { contract } from 'contract';
import { User } from './entities/user.entity';
import { UserRole } from 'contract/enum';
export const userContractController = nestControllerContract(contract.user);
export type UserRequestShapes = NestRequestShapes<
  typeof userContractController
>;

@Controller()
export class UserController
  implements NestControllerInterface<typeof userContractController>
{
  constructor(private userService: UserService) {}

  @TsRest(userContractController.createJobseeker)
  async createJobseeker(
    @TsRestRequest() { body }: UserRequestShapes['createJobseeker'],
  ) {
    const data = await this.userService.createJobseeker(body);
    return {
      status: 200 as const,
      body: {
        isSuccess: true,
        message: 'Profile Created Successfully!!!',
        token: data.token,
      },
    };
  }
  @TsRest(userContractController.createEmployer)
  async createEmployer(
    @TsRestRequest() { body }: UserRequestShapes['createEmployer'],
  ) {
    const data = await this.userService.createEmployer(body);
    return {
      status: 200 as const,
      body: {
        isSuccess: true,
        message: 'Profile Created Successfully!!!',
        token: data.token,
      },
    };
  }

  @JobSeekerOnlyAuth()
  @TsRest(userContractController.updateJobseeker)
  updateJobseeker(
    @getUserFromToken() user: User,
    @TsRestRequest() { body }: UserRequestShapes['updateJobseeker'],
  ) {
    return this.userService.updateJobseeker(user, body);
  }

  @EmployerOnlyAuth()
  @TsRest(userContractController.updateEmployer)
  updateEmployer(
    @getUserFromToken() user: User,
    @TsRestRequest() { body }: UserRequestShapes['updateEmployer'],
  ) {
    return this.userService.updateEmployer(body, user);
  }

  @JobSeekerOnlyAuth()
  @TsRest(userContractController.getJobseekerProfile)
  async getJobseekerProfile(@getUserFromToken() user: User) {
    const jobSeeker = (await this.userService.getJobseekerProfile(user))
      .jobSeeker;

    if (!jobSeeker) {
      throw new BadRequestException('JobSeeker not found');
    }
    return {
      status: 200 as const,
      body: {
        firstName: user.firstName,
        lastName: user.lastName,
        email: user.email,
        phone: user.phone,
        picture: user.picture,
        id: user.id,
        profileSummary: jobSeeker.profileSummary,
        headline: jobSeeker.headline,
        expectedSalaryInLpa: jobSeeker.expectedSalaryInLpa,
        isPrivate: jobSeeker.isPrivate,
        externalLink: jobSeeker.externalLink,
        resume: jobSeeker.resume,
        videoResume: jobSeeker.videoResume,
        socialMediaLink: jobSeeker.socialMediaLink,
        preferredLocations: jobSeeker.preferredLocations.$.getItems().map(
          (c) => ({ id: c.id, name: c.name }),
        ),
        experienceInYear: jobSeeker.experienceInYear,
        languages: jobSeeker.languages.$.getItems().map((c) => ({
          id: c.id,
          name: c.name,
        })),
        subfunction: jobSeeker.subfunction.$.getItems().map((c) => ({
          id: c.id,
          name: c.name,
        })),
        city: {
          id: jobSeeker.city.id,
          name: jobSeeker.city.name,
        },
        noticePeriod: jobSeeker.noticePeriod,
        workSchedule: jobSeeker.workSchedule,
        skills: jobSeeker.skills.$.getItems().map((s) => ({
          id: s.id,
          name: s.name,
        })),
        isSubscribedToAlerts: jobSeeker.isSubscribedToAlerts,
      },
    };
  }

  @Auth()
  @TsRest(userContractController.getUserProfile)
  async getUserProfile(@getUserFromToken() user: User) {
    const userObj = {
      firstName: user.firstName,
      lastName: user.lastName,
      email: user.email,
      phone: user.phone,
      picture: user.picture,
      id: user.id,
      role: user.role,
      isVerified: Boolean(user.employer?.isVerified),
    };

    if (user.role !== UserRole.employer) {
      return {
        status: 200 as const,
        body: userObj,
      };
    }
    if (!user.employer) {
      throw new BadRequestException('Employer not found');
    }

    const [employerProps, employerCompany] = await Promise.all([
      this.userService.hasUserEverPurchasedSubscription({
        employer: user.employer,
      }),
      this.getEmployerById(user).then((res) => res.body.company.name),
    ]);

    return {
      status: 200 as const,
      body: {
        ...userObj,
        ...employerProps,
        ...{
          companyName: employerCompany,
        },
      },
    };
  }
  @EmployerOnlyAuth()
  @TsRest(userContractController.getEmployerById)
  getEmployerById(@getUserFromToken() user: User) {
    return this.userService.getEmployerById(user);
  }

  @EmployerOnlyAuth()
  @TsRest(userContractController.viewJobseekerProfileFromManageApplication)
  async viewJobseekerProfileFromManageApplication(
    @TsRestRequest()
    { query }: UserRequestShapes['viewJobseekerProfileFromManageApplication'],
  ) {
    const jobSeeker =
      await this.userService.viewJobseekerProfileFromManageApplication(query);
    return {
      status: 200 as const,
      body: {
        firstName: jobSeeker.user.firstName,
        lastName: jobSeeker.user.lastName,
        email: jobSeeker.user.email,
        phone: jobSeeker.user.phone,
        picture: jobSeeker.user.picture,
        id: jobSeeker.user.id,
        profileSummary: jobSeeker.profileSummary,
        headline: jobSeeker.headline,
        expectedSalaryInLpa: jobSeeker.expectedSalaryInLpa,
        isPrivate: jobSeeker.isPrivate,
        externalLink: jobSeeker.externalLink,
        resume: jobSeeker.resume,
        videoResume: jobSeeker.videoResume,
        socialMediaLink: jobSeeker.socialMediaLink,
        preferredLocations: jobSeeker.preferredLocations.$.getItems().map(
          (c) => ({ id: c.id, name: c.name }),
        ),
        experienceInYear: jobSeeker.experienceInYear,
        languages: jobSeeker.languages.$.getItems().map((c) => ({
          id: c.id,
          name: c.name,
        })),
        subfunction: jobSeeker.subfunction.$.getItems().map((c) => ({
          id: c.id,
          name: c.name,
        })),
        city: {
          id: jobSeeker.city.id,
          name: jobSeeker.city.name,
        },
        noticePeriod: jobSeeker.noticePeriod,
        workSchedule: jobSeeker.workSchedule,
        skills: jobSeeker.skills.$.getItems().map((s) => ({
          id: s.id,
          name: s.name,
        })),
        isSubscribedToAlerts: jobSeeker.isSubscribedToAlerts,
      },
    };
  }
}
