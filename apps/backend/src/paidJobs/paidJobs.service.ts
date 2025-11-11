import { EntityManager } from '@mikro-orm/postgresql';
import { ForbiddenException, Injectable } from '@nestjs/common';
import { NestRequestShapes, nestControllerContract } from '@ts-rest/nest';
import { paidJobsContract } from '../../../contract/paidJobs/contract';
import { PaidJobsShapes } from './paidJobs.controller';
import { Employer } from 'src/user/entities/employer.entity';
import { PaidJobSubscriptionPlan } from './entities/paidJobSubscriptionPlan.entity';
import { PaidJobSubscriptionOrder } from './entities/paidJobSubscriptionOrder.entity';
import { PaymentStatusEnum } from '../../../contract/enum';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import { EmployerPaidJobSubscription } from './entities/employerPaidJobSubscription.entity';
import {
  FilterQuery,
  FindOneOptions,
  LockMode,
  QueryOrder,
  wrap,
} from '@mikro-orm/core';
import { JobBoost } from './entities/jobBoost.entity';
import { Job } from 'src/job/entities/job.entity';
import { createPaginatedResponse, getDatefromDifference } from 'src/utils';
import { SubscriptionService } from 'src/subscription/subscriptions.service';
import { CronExpressions, Schedule } from 'src/pgschedule/pgschedule.decorator';

export const paidJobsContractController =
  nestControllerContract(paidJobsContract);
export type PaidJobShapes = NestRequestShapes<
  typeof paidJobsContractController
>;

@Injectable()
export class PaidJobsService {
  constructor(
    private readonly em: EntityManager,
    @InjectPinoLogger(PaidJobsService.name) private logger: PinoLogger,
    private subscriptionBaseService: SubscriptionService,
  ) {}

  public async getSubscriptionPlans() {
    return this.subscriptionBaseService.getSubscriptionPlans<PaidJobSubscriptionPlan>(
      PaidJobSubscriptionPlan,
    );
  }

  public async createOrder(
    props: PaidJobsShapes['getOrder']['body'],
    { employer }: { employer: Employer },
  ) {
    const { subDetail, invoice } =
      await this.subscriptionBaseService.createRazorpayOrderFromPlan(
        PaidJobSubscriptionPlan,
        props.subscriptionPlanId,
      );

    const newOrder = new PaidJobSubscriptionOrder({
      employer,
      invoiceLink: invoice.short_url ? invoice.short_url : '',
      plan: subDetail,
      razorpayInvoiceId: invoice.id,
      razorpayOrderId: invoice.order_id ? invoice.order_id : '',
    });

    await this.em.persistAndFlush(newOrder);

    return { orderId: invoice.order_id };
  }

  public async checkAndUpdateEmployerSubscription(
    props: PaidJobShapes['updateSubscription']['body'],
  ) {
    const order = await this.em.findOneOrFail(
      PaidJobSubscriptionOrder,
      {
        razorpayInvoiceId: props.razorpay_invoice_id,
      },
      {
        populate: ['employer', 'employer.user', 'plan'],
      },
    );

    if (props.razorpay_invoice_status !== 'paid') {
      this.logger.info('invalid payment', props);

      wrap(order).assign({
        paymentStatus: PaymentStatusEnum.failure,
        razorpayPaymentId: props.razorpay_payment_id,
      });

      await this.em.flush();

      return false;
    }

    const paymentDetails = await this.subscriptionBaseService.getPaymentDetails(
      props.razorpay_payment_id,
    );

    wrap(order).assign({
      paymentStatus: PaymentStatusEnum.success,
      paymentMethod: paymentDetails.method,
      razorpayPaymentId: props.razorpay_payment_id,
    });

    const employerSubscriptionObj = new EmployerPaidJobSubscription({
      order,
    });

    await this.em.persistAndFlush(employerSubscriptionObj);

    // await this.subscriptionBaseService.sendSubscriptionEmail({
    //   email: order.employer.user.email,
    //   transactionId: props.razorpay_payment_id,
    //   paymentDate: new Date(),
    //   totalAmount: order.paidAmount,
    //   name: order.employer.user.fullName,
    //   subscriptionType: 'Paid Job Boost',
    // });

    return true;
  }

  public async getSubscriptionHistory(
    props: PaidJobShapes['getSubscriptionHistory']['query'],
    { employer }: { employer: Employer },
  ) {
    const { pageNumber, pageSize } = props;

    const [ordersRawObject, totalRows] = await Promise.all([
      this.em.find(
        PaidJobSubscriptionOrder,
        {
          employer,
        },
        {
          populate: ['plan'],
          limit: pageSize,
          offset: (pageNumber - 1) * pageSize,
          orderBy: { createdAt: QueryOrder.DESC },
        },
      ),
      this.em.count(EmployerPaidJobSubscription, {
        employer,
      }),
    ]);

    const orders = ordersRawObject.map((order) => {
      return {
        id: order.id,
        subscriptionName: order.planName,
        createdAt: order.createdAt,
        paidAmount: order.paidAmount,
        paymentMethod: order.paymentMethod,
        paymentStatus: order.paymentStatus,
        invoiceLink: order.invoiceLink,
        boostLimit: order.boostLimit,
        boostDays: order.boostDays,
        validForDays: order.validForDays,
        validTill: getDatefromDifference(order.validForDays, order.createdAt),
      };
    });

    return {
      currentPageNumber: pageNumber,
      currentPageSize: pageSize,
      totalItems: orders.length,
      totalPages: Math.ceil(totalRows / pageSize),
      results: orders,
    };
  }

  public async getActiveSubscriptions(
    props: PaidJobShapes['getActiveSubscriptions']['query'],
    { employer }: { employer: Employer },
  ) {
    const { pageNumber, pageSize } = props;

    const [subscriptionObj, totalRows] = await Promise.all([
      this.em.find(
        EmployerPaidJobSubscription,
        this.subscriptionBaseService.getActiveSubscriptionOptions(employer),
        {
          populate: ['order', 'plan', 'jobBoosts'],
          limit: pageSize,
          offset: (pageNumber - 1) * pageSize,
          orderBy: { createdAt: QueryOrder.DESC },
        },
      ),
      this.em.count(
        EmployerPaidJobSubscription,
        this.subscriptionBaseService.getActiveSubscriptionOptions(employer),
      ),
    ]);

    const subscriptions = subscriptionObj.map((sub) => {
      const boostLeft = sub.boostLimit - sub.usedJobBoosts.length;
      return {
        id: sub.id,
        subscriptionName: sub.planName,
        createdAt: sub.order.createdAt,
        expiryDate: sub.expiryDate,
        validForDays: sub.validForDays,
        totalBoosts: sub.boostLimit,
        boostLimit: boostLeft > 0 ? boostLeft : 0,
        boostDays: sub.boostDays,
        boostUsed: sub.usedJobBoosts.length,
        validTill: sub.expiryDate,
        paymentStatus: PaymentStatusEnum.success,
      };
    });

    return {
      currentPageNumber: pageNumber,
      currentPageSize: pageSize,
      totalItems: subscriptions.length,
      totalPages: Math.ceil(totalRows / pageSize),
      results: subscriptions,
    };
  }

  public async boostJob(
    props: PaidJobShapes['boostJob']['body'],
    { employer }: { employer: Employer },
  ) {
    const job = await this.em.findOneOrFail(Job, props.jobId);

    const isJobAlreadyBoosted = await this.em.count(JobBoost, {
      job,
      employer,
      $or: [
        {
          boostEndDate: {
            $gte: new Date(),
          },
        },
        {
          boostEndDate: null,
        },
      ],
    });

    if (isJobAlreadyBoosted) {
      throw new ForbiddenException('Job already boosted');
    }

    await this.em.transactional(async (em) => {
      await this.createJobBoost(props.subscriptionId, { employer, job }, em, {
        lockMode: LockMode.PESSIMISTIC_WRITE,
      });
      await em.flush();
    });

    return {
      isSuccess: true,
      message: 'ok',
    };
  }

  public async deleteJobBoost(options: FilterQuery<JobBoost>) {
    const jobBoost = await this.em.findOne(JobBoost, options);

    if (jobBoost) {
      wrap(jobBoost).assign({
        isDeleted: true,
      });
    }
  }

  public async createJobBoost(
    subscriptionId: number,
    options: { employer: Employer; job?: Job },
    newEm?: EntityManager,
    lockOptions?: FindOneOptions<EmployerPaidJobSubscription>,
  ) {
    const em = newEm ?? this.em;

    const [subDetails, jobBoosts] = await Promise.all([
      em.findOneOrFail(
        EmployerPaidJobSubscription,
        this.subscriptionBaseService.getActiveSubscriptionOptions<EmployerPaidJobSubscription>(
          options.employer,
          {
            id: subscriptionId,
          },
        ),
        lockOptions,
      ),
      em.find(JobBoost, {
        subscription: this.em.getReference(
          EmployerPaidJobSubscription,
          subscriptionId,
        ),
        isDeleted: false,
      }),
    ]);

    if (subDetails.boostLimit - jobBoosts.length <= 0) {
      throw new ForbiddenException('No more job boost left');
    }

    const isAdminApproved = options.job ? options.job.isAdminApproved : false;
    const subscription = subDetails;

    const newJobBoost = new JobBoost({
      boostStartDate: isAdminApproved ? new Date() : null,
      boostEndDate: isAdminApproved
        ? getDatefromDifference(subscription.boostDays)
        : null,
      ...options,
      subscription: subDetails,
    });

    em.persist(newJobBoost);
  }

  public async getLastPaymentDetails({ employer }: { employer: Employer }) {
    const lastPaymentDetails = await this.em.findOne(
      PaidJobSubscriptionOrder,
      {
        employer,
        paymentStatus: PaymentStatusEnum.success,
      },
      {
        orderBy: { createdAt: QueryOrder.DESC },
      },
    );

    // made them optional so that contract can be fulfilled
    return {
      reference_no: lastPaymentDetails?.razorpayInvoiceId
        ? lastPaymentDetails.razorpayInvoiceId
        : '',
      payment_date: lastPaymentDetails?.updatedAt
        ? lastPaymentDetails.updatedAt
        : '',
      payment_method: lastPaymentDetails?.paymentMethod
        ? lastPaymentDetails.paymentMethod
        : '',
      subscription_validity: lastPaymentDetails?.validForDays
        ? lastPaymentDetails.validForDays
        : '',
      invoice_link: lastPaymentDetails?.invoiceLink
        ? lastPaymentDetails.invoiceLink
        : '',
      amount: lastPaymentDetails?.paidAmount
        ? lastPaymentDetails.paidAmount
        : 0,
    };
  }

  @Schedule('updatePaymentStatus', CronExpressions.everyFiveMinutes)
  async updatePaymentStatus() {
    await this.subscriptionBaseService.updateSubscriptions(
      PaidJobSubscriptionOrder,
      (order) => {
        const subscription = new EmployerPaidJobSubscription({ order });
        this.em.persist(subscription);
      },
    );
  }

  @Schedule('updateJobBoostDates', CronExpressions.everyMinute)
  async updateJobBoostDates() {
    const jobBoosts = await this.em.find(
      JobBoost,
      {
        boostEndDate: null,
        $or: [{ job: { isAdminApproved: true } }],
      },
      {
        populate: ['subscription'],
      },
    );

    jobBoosts.map((jb) => {
      wrap(jb).assign({
        boostStartDate: new Date(),
        boostEndDate: getDatefromDifference(jb.subscription.boostDays),
      });
    });

    await this.em.flush();
  }

  public async activateJobBoost(options: { job: Job }) {
    const jobBoost = await this.em.findOne(
      JobBoost,
      {
        ...options,
        boostStartDate: null, // select a inactive job boost
      },
      {
        populate: ['subscription'],
      },
    );

    if (jobBoost) {
      wrap(jobBoost).assign({
        boostStartDate: new Date(),
        boostEndDate: getDatefromDifference(jobBoost.subscription.boostDays),
      });
    }
  }

  public async getBoostedJobForSubscription(
    props: PaidJobShapes['getBoostedJobsForSubscription']['query'],
  ) {
    const { pageNumber, pageSize, subscriptionId } = props;

    const [jobBoosts, count] = await this.em.findAndCount(
      JobBoost,
      {
        subscription: {
          id: subscriptionId,
        },
        isDeleted: false,
      },
      {
        fields: ['boostEndDate', 'boostStartDate', 'job.title'],
        limit: pageSize,
        offset: (pageNumber - 1) * pageSize,
        filters: false, // to disable delete job filter, we need to show deleted jobs also
      },
    );

    return createPaginatedResponse({
      results: jobBoosts.map((job) => ({
        boostEndDate: job.boostEndDate,
        boostStartDate: job.boostStartDate,
        title: job.job ? job.job.title : '',
      })),
      pageNumber,
      pageSize,
      totalCount: count,
    });
  }
}
