import { Controller, ForbiddenException, Redirect } from '@nestjs/common';
import { PaidJobShapes, PaidJobsService } from './paidJobs.service';
import {
  NestRequestShapes,
  TsRest,
  TsRestRequest,
  nestControllerContract,
} from '@ts-rest/nest';
import { paidJobsContract } from '../../../contract/paidJobs/contract';
import {
  EmployerOnlyAuth,
  getUserFromToken,
} from 'src/common/decorators/user.decorator';
import { User } from 'src/user/entities/user.entity';
import { ConfigService } from '@nestjs/config';
import { LoadedUser } from 'src/user/user.service';

export const paidJobsContractController =
  nestControllerContract(paidJobsContract);
export type PaidJobsShapes = NestRequestShapes<
  typeof paidJobsContractController
>;

@Controller()
export class PaidJobsController {
  constructor(
    private readonly paidJobsService: PaidJobsService,
    private configService: ConfigService,
  ) {}

  @TsRest(paidJobsContractController.getSubscriptionPlans)
  async getSubscriptionPlans() {
    return this.paidJobsService.getSubscriptionPlans();
  }

  @EmployerOnlyAuth()
  @TsRest(paidJobsContractController.getOrder)
  async getOrder(
    @TsRestRequest() { body }: PaidJobsShapes['getOrder'],
    @getUserFromToken() user: LoadedUser,
  ) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }
    return this.paidJobsService.createOrder(body, { employer: user.employer });
  }

  @TsRest(paidJobsContractController.updateSubscription)
  @Redirect()
  async updateSubscriptions(
    @TsRestRequest() { body }: PaidJobsShapes['updateSubscription'],
  ) {
    const isSuccess =
      await this.paidJobsService.checkAndUpdateEmployerSubscription(body);

    if (isSuccess) {
      return {
        statusCode: 302,
        url: this.configService.get<string>('frontEndUrl') + '/paidJobThankYou',
      };
    }
    return {
      statusCode: 302,
      url: this.configService.get<string>('frontEndUrl') + '/failure',
    };
  }

  @EmployerOnlyAuth()
  @TsRest(paidJobsContractController.getSubscriptionHistory)
  async getSubscriptionHistory(
    @TsRestRequest() { query }: PaidJobsShapes['getSubscriptionHistory'],
    @getUserFromToken() user: LoadedUser,
  ) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }

    return this.paidJobsService.getSubscriptionHistory(query, {
      employer: user.employer,
    });
  }

  @EmployerOnlyAuth()
  @TsRest(paidJobsContractController.boostJob)
  async boostJob(
    @TsRestRequest() { body }: PaidJobsShapes['boostJob'],
    @getUserFromToken() user: LoadedUser,
  ) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }

    return this.paidJobsService.boostJob(body, { employer: user.employer });
  }

  @EmployerOnlyAuth()
  @TsRest(paidJobsContractController.getActiveSubscriptions)
  async getActiveSubscriptions(
    @TsRestRequest() { query }: PaidJobShapes['getActiveSubscriptions'],
    @getUserFromToken() user: LoadedUser,
  ) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }
    return this.paidJobsService.getActiveSubscriptions(query, {
      employer: user.employer,
    });
  }

  @EmployerOnlyAuth()
  @TsRest(paidJobsContractController.getLastPaymentDetails)
  async getLastPaymentDetails(@getUserFromToken() user: User) {
    if (!user.employer) {
      throw new ForbiddenException('You are not a employer');
    }
    return this.paidJobsService.getLastPaymentDetails({
      employer: user.employer,
    });
  }

  // jobSeekerThankYou created because frontend needed a clone route for a diff page
  @TsRest(paidJobsContractController.updateSubscriptionFromJobForm)
  @Redirect()
  async updateSubscriptionFromJobForm(
    @TsRestRequest() { body }: PaidJobsShapes['updateSubscriptionFromJobForm'],
  ) {
    const isSuccess =
      await this.paidJobsService.checkAndUpdateEmployerSubscription(body);

    if (isSuccess) {
      return {
        statusCode: 302,
        url:
          this.configService.get<string>('frontEndUrl') + '/jobSeekerThankYou',
      };
    }
    return {
      statusCode: 302,
      url: this.configService.get<string>('frontEndUrl') + '/failure',
    };
  }

  @TsRest(paidJobsContractController.getBoostedJobsForSubscription)
  async getBoostedJobsForSubscription(
    @TsRestRequest() { query }: PaidJobsShapes['getBoostedJobsForSubscription'],
  ) {
    return this.paidJobsService.getBoostedJobForSubscription(query);
  }
}
