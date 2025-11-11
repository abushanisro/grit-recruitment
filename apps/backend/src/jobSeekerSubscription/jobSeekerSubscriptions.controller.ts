import { Controller, ForbiddenException, Redirect } from '@nestjs/common';
import { JobSeekerSubscriptionService } from './jobSeekerSubscriptions.service';
import {
  EmployerOnlyAuth,
  getUserFromToken,
} from 'src/common/decorators/user.decorator';
import {
  NestRequestShapes,
  TsRest,
  TsRestRequest,
  nestControllerContract,
} from '@ts-rest/nest';
import { subscriptionContract } from '../../../contract/subscription/contract';
import { User } from 'src/user/entities/user.entity';
import { ConfigService } from '@nestjs/config';
import { LoadedUser } from 'src/user/user.service';

export const subscriptionContractController =
  nestControllerContract(subscriptionContract);
export type SubscriptionShapes = NestRequestShapes<
  typeof subscriptionContractController
>;

@Controller()
export class JobSeekerSubscriptionsController {
  constructor(
    private readonly subscriptionsService: JobSeekerSubscriptionService,
    private configService: ConfigService,
  ) {}

  @TsRest(subscriptionContractController.getOrder)
  @EmployerOnlyAuth()
  async getOrder(
    @TsRestRequest() { body }: SubscriptionShapes['getOrder'],
    @getUserFromToken() user: LoadedUser,
  ) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }
    return this.subscriptionsService.createOrder(body, {
      employer: user.employer,
    });
  }

  @TsRest(subscriptionContractController.getSubscriptionPlans)
  async getSubscriptionPlans() {
    return this.subscriptionsService.getSubscriptionPlans();
  }

  @EmployerOnlyAuth()
  @TsRest(subscriptionContractController.getJobSeekerProfile)
  async getJobSeekerProfile(
    @TsRestRequest() { query }: SubscriptionShapes['getJobSeekerProfile'],
    @getUserFromToken() user: LoadedUser,
  ) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }
    return this.subscriptionsService.getJobSeekerProfile({
      employer: user.employer,
      employerProfileViewId: query.employerProfileViewId,
    });
  }

  @TsRest(subscriptionContractController.updateSubscription)
  @Redirect()
  async updateSubscriptions(
    @TsRestRequest() { body }: SubscriptionShapes['updateSubscription'],
  ) {
    const isSuccess =
      await this.subscriptionsService.checkAndUpdateEmployerSubscription(body);

    if (isSuccess) {
      return {
        statusCode: 302,
        url: this.configService.get<string>('frontEndUrl') + '/thankYou',
      };
    }
    return {
      statusCode: 302,
      url: this.configService.get<string>('frontEndUrl') + '/failure',
    };
  }

  @TsRest(subscriptionContractController.getActiveSubscriptions)
  @EmployerOnlyAuth()
  async getCurrentSubscriptions(
    @TsRestRequest() { query }: SubscriptionShapes['getActiveSubscriptions'],
    @getUserFromToken() user: LoadedUser,
  ) {
    return this.subscriptionsService.getCurrentSubscription(query, {
      userId: user.id,
    });
  }

  @TsRest(subscriptionContractController.getSubscriptionHistory)
  @EmployerOnlyAuth()
  async getSubscriptionHistory(
    @TsRestRequest() { query }: SubscriptionShapes['getSubscriptionHistory'],
    @getUserFromToken() user: LoadedUser,
  ) {
    return this.subscriptionsService.getSubscriptionHistory(query, {
      userId: user.id,
    });
  }

  @TsRest(subscriptionContractController.getAllJobsFromEmployer)
  @EmployerOnlyAuth()
  async getAllJobsFromEmployer(
    @getUserFromToken() user: LoadedUser,
    @TsRestRequest() { query }: SubscriptionShapes['getAllJobsFromEmployer'],
  ) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }
    return this.subscriptionsService.getAllEmployerCreatedJobs(query, {
      employer: user.employer,
    });
  }

  @TsRest(subscriptionContractController.inviteUsers)
  @EmployerOnlyAuth()
  async inviteUser(
    @TsRestRequest() { body }: SubscriptionShapes['inviteUsers'],
    @getUserFromToken() user: LoadedUser,
  ) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }
    return this.subscriptionsService.inviteBulkUsersForJob(body, {
      employer: user.employer,
    });
  }

  @TsRest(subscriptionContractController.getEmployerProfileView)
  @EmployerOnlyAuth()
  async getEmployerProfileView(
    @TsRestRequest() { body }: SubscriptionShapes['getEmployerProfileView'],
    @getUserFromToken() user: LoadedUser,
  ) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }

    return this.subscriptionsService.createEmployerProfileView({
      jobSeekerId: body.jobSeekerId,
      subscriptionId: body.subscriptionId,
      employer: user.employer,
    });
  }

  @TsRest(subscriptionContractController.filterDirectory)
  @EmployerOnlyAuth()
  async filterJobSeekers(
    @TsRestRequest() { query }: SubscriptionShapes['filterDirectory'],
    @getUserFromToken() user: LoadedUser,
  ) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }
    return this.subscriptionsService.filterJobSeekerDirectory(query, {
      employer: user.employer,
    });
  }

  @TsRest(subscriptionContractController.getViewedUsersForSubscription)
  @EmployerOnlyAuth()
  async getAllViewedUsersForSubscription(
    @TsRestRequest()
    { query }: SubscriptionShapes['getViewedUsersForSubscription'],
  ) {
    const jobSeekers =
      await this.subscriptionsService.getViewedUsersForSubscription(query);
    return jobSeekers;
  }

  @EmployerOnlyAuth()
  @TsRest(subscriptionContractController.getLastPaymentDetails)
  async getLastPaymentDetails(@getUserFromToken() user: User) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }
    return this.subscriptionsService.getLastPaymentDetails({
      employer: user.employer,
    });
  }

  @EmployerOnlyAuth()
  @TsRest(subscriptionContractController.suggestionActiveSubscriptions)
  async suggestionActiveSubscriptions(@getUserFromToken() user: User) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }
    return this.subscriptionsService.suggestionActiveSubscriptions({
      employer: user.employer,
    });
  }
}
