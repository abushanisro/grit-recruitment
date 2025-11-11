import { EntityManager } from '@mikro-orm/postgresql';
import {
  BadRequestException,
  ForbiddenException,
  Injectable,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { NestRequestShapes, nestControllerContract } from '@ts-rest/nest';
import { subscriptionContract } from '../../../contract/subscription/contract';
import { EmployerProfileViews } from './entities/employerProfileViews.entity';
import { EmployerJobSeekerSubscription } from './entities/employerJobSeekerSubscription.entity';
import { JobSeekerSubscriptionOrder } from './entities/jobSeekerSubscriptionOrder.entity';
import { JobSeekerSubscriptionPlan } from './entities/jobSeekerSubscriptionPlan.entity';
import { User } from 'src/user/entities/user.entity';
import { Job } from 'src/job/entities/job.entity';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import {
  EmailStatusEnum,
  PaymentStatusEnum,
  UserRole,
} from '../../../contract/enum';
import { EmailService } from 'src/auth/email.service';
import { Employer } from 'src/user/entities/employer.entity';
import { FilterQuery, LockMode, QueryOrder, wrap } from '@mikro-orm/core';
import { render } from '@react-email/render';
import { JobSeeker } from 'src/user/entities/jobseeker.entity';
import { EmployerJobSeekerInvite } from './entities/employerJobSeekerInvite.entity';
import { isNil, isNumber, omit } from 'lodash';
import { inviteEmail } from './emails/jobInvite';
import {
  createPaginatedResponse,
  generateJobIdPageUrl,
  getExperienceFilterForJobSeeker,
} from 'src/utils';
import { SubscriptionService } from 'src/subscription/subscriptions.service';
import {
  CronExpressions,
  QueueMessageHandler,
  Schedule,
} from 'src/pgschedule/pgschedule.decorator';
import { QueueService } from 'src/pgschedule/queue.service';
import {
  QueueNamesEnum,
  QueuePayloadData,
} from 'src/pgschedule/config/queueConfig';

export const subscriptionContractController =
  nestControllerContract(subscriptionContract);
export type SubscriptionShapes = NestRequestShapes<
  typeof subscriptionContractController
>;

@Injectable()
export class JobSeekerSubscriptionService {
  constructor(
    private readonly em: EntityManager,
    private configService: ConfigService,
    @InjectPinoLogger(JobSeekerSubscriptionService.name)
    private logger: PinoLogger,
    private emailService: EmailService,
    private subscriptionBaseService: SubscriptionService,
    private queueService: QueueService,
  ) {}

  frontEndUrl = this.configService.get<string>('frontEndUrl');

  public async getSubscriptionPlans() {
    return this.subscriptionBaseService.getSubscriptionPlans<JobSeekerSubscriptionPlan>(
      JobSeekerSubscriptionPlan,
    );
  }

  public async createOrder(
    props: SubscriptionShapes['getOrder']['body'],
    { employer }: { employer: Employer },
  ) {
    const { subscriptionPlanId } = props;

    const { subDetail, invoice } =
      await this.subscriptionBaseService.createRazorpayOrderFromPlan(
        JobSeekerSubscriptionPlan,
        subscriptionPlanId,
      );

    const newOrder = new JobSeekerSubscriptionOrder({
      razorpayInvoiceId: invoice.id,
      razorpayOrderId: invoice.order_id ? invoice.order_id : '',
      invoiceLink: invoice.short_url ? invoice.short_url : '',
      plan: subDetail,
      employer,
    });
    await this.em.persistAndFlush(newOrder);

    return { orderId: invoice.order_id };
  }

  public async checkAndUpdateEmployerSubscription(
    props: SubscriptionShapes['updateSubscription']['body'],
  ) {
    if (props.razorpay_invoice_status !== 'paid') {
      this.logger.info('invalid payment', props);
      return false;
    }

    const paymentDetails = await this.subscriptionBaseService.getPaymentDetails(
      props.razorpay_payment_id,
    );

    const order = await this.em.findOneOrFail(
      JobSeekerSubscriptionOrder,
      {
        razorpayInvoiceId: props.razorpay_invoice_id,
      },
      { populate: ['employer', 'employer.user'] },
    );

    wrap(order).assign({
      paymentStatus: PaymentStatusEnum.success,
      paymentMethod: paymentDetails.method,
      razorpayPaymentId: props.razorpay_payment_id,
    });

    const employerSubscriptionObj = new EmployerJobSeekerSubscription({
      order,
    });

    await this.em.persistAndFlush(employerSubscriptionObj);

    // await this.subscriptionBaseService.sendSubscriptionEmail({
    //   email: order.employer.user.email,
    //   transactionId: props.razorpay_payment_id,
    //   paymentDate: new Date(),
    //   totalAmount: order.paidAmount,
    //   name: order.employer.user.fullName,
    //   subscriptionType: 'JobSeeker Directory',
    // });

    return true;
  }

  public async inviteBulkUsersForJob(
    props: SubscriptionShapes['inviteUsers']['body'],
    { employer }: { employer: Employer },
  ) {
    const { jobId, jobSeekerIds, subscriptionId } = props;
    const jobDetails = await this.em.findOneOrFail(Job, jobId, {
      populate: ['cities', 'company'],
    });

    const jobSeekers = await this.em.find(
      JobSeeker,
      { user: jobSeekerIds },
      { populate: ['user'] },
    );

    const invites = jobSeekers.map(
      (jobSeeker) =>
        new EmployerJobSeekerInvite({
          employer,
          job: jobDetails,
          jobSeeker: jobSeeker,
        }),
    );

    this.em.persist(invites);

    await this.createBulkEmployerProfileView({
      jobSeekers,
      subscriptionId,
      employer,
    });

    await Promise.all(
      invites.map((i) =>
        this.queueService.sendMessage({
          name: QueueNamesEnum.jobInvite,
          data: {
            inviteId: i.id,
          },
        }),
      ),
    );

    return {
      isSuccess: true,
      message: 'ok',
    };
  }

  public async getSubscriptionHistory(
    props: SubscriptionShapes['getSubscriptionHistory']['query'],
    { userId }: { userId: number },
  ) {
    const { pageSize, pageNumber } = props;

    let subscriptionHistory = await this.em
      .getKnex()
      .select(
        'subscription_history.*',
        this.em
          .getKnex()
          .raw(
            '(SELECT COUNT(*) FROM job_seeker_subscription_order where employer_user_id = ?) AS subscription_count',
            [userId],
          ),
      )
      .from(
        this.em
          .getKnex()
          .select(
            's.id',
            's.created_at as createdAt',
            's.paid_amount as paidAmount',
            's.payment_method as paymentMethod',
            's.payment_status as paymentStatus',
            's.invoice_link as invoiceLink',
            'sp.name as subscriptionName',
            's.allowed_profile_count as allowedProfileCount',
            'sp.valid_for_days as validForDays',
            this.em
              .getKnex()
              .raw(
                `(s.created_at + INTERVAL '1 day' * sp.valid_for_days) as "validTill"`,
              ),

            this.em
              .getKnex()
              .raw('COUNT(DISTINCT epv.id) as "profileViewCount"'),
          )
          .from('job_seeker_subscription_order as s')
          .join(
            'employer_job_seeker_subscription as es',
            's.employer_user_id',
            '=',
            'es.employer_user_id',
          )
          .join('job_seeker_subscription_plan as sp', 'sp.id', '=', 's.plan_id')
          .leftJoin('employer_profile_views as epv', function () {
            this.on('epv.employer_user_id', '=', 's.employer_user_id').andOn(
              's.plan_id',
              '=',
              'sp.id',
            );
          })
          .where('s.employer_user_id', userId)
          .groupBy(
            's.id',
            's.created_at',
            's.paid_amount',
            's.payment_method',
            's.payment_status',
            's.invoice_link',
            'sp.name',
            's.allowed_profile_count',
            'sp.valid_for_days',
          )
          .orderBy('s.id', 'desc')
          .as('subscription_history'),
      )
      .limit(pageSize)
      .offset(pageSize * (pageNumber - 1));

    const totalPages = subscriptionHistory[0]?.subscription_count
      ? subscriptionHistory[0].subscription_count
      : 0;
    // remove this subscription_count property from each element of subscriptionHistory

    subscriptionHistory = subscriptionHistory.map((h) =>
      omit(h, 'subscription_count'),
    );

    return {
      results: subscriptionHistory,
      totalPages: Math.ceil(totalPages / pageSize),
      totalItems: subscriptionHistory.length,
      currentPageNumber: pageNumber,
      currentPageSize: pageSize,
    };
  }

  public async getCurrentSubscription(
    props: SubscriptionShapes['getActiveSubscriptions']['query'],
    { userId }: { userId: number },
  ) {
    const { pageNumber, pageSize } = props;

    const currentSubscription = await this.em
      .getKnex()
      .select(
        'sp.name as subscriptionName',
        'es.id',
        'es.created_at as createdAt',
        'es.expiry_date as expiryDate',
        'sp.valid_for_days as validForDays',
        this.em.getKnex().raw('\'success\' as "paymentStatus"'),
        this.em
          .getKnex()
          .raw(
            `(Select count(DISTINCT (epv.job_seeker_user_id)) from employer_profile_views epv where epv.employer_subscription_id = cast(es.id as integer)) as "viewCounts"`,
          ),
        this.em
          .getKnex()
          .raw(
            `(es.allowed_profile_view_count - (Select count(DISTINCT (epv.employer_user_id, epv.job_seeker_user_id, epv.employer_subscription_id)) from employer_profile_views epv where epv.employer_subscription_id = cast(es.id as integer))) as "pendingProfileView"`,
          ),
      )
      .from('job_seeker_subscription_plan as sp')
      .join('employer_job_seeker_subscription as es', 'sp.id', 'es.plan_id')
      .where('es.expiry_date', '>', this.em.getKnex().fn.now())
      .where('es.employer_user_id', userId)
      .orderBy('es.expiry_date', 'desc')
      .limit(pageSize)
      .offset(pageSize * (pageNumber - 1));

    const totalPages = await this.em
      .getKnex()
      .select(
        this.em
          .getKnex()
          .raw(
            'COUNT(*) from employer_job_seeker_subscription where employer_user_id = ? and expiry_date >= now()',
            [userId],
          ),
      );

    return {
      results: currentSubscription,
      totalPages: Math.ceil(totalPages[0].count / pageSize),
      totalItems: currentSubscription.length,
      currentPageNumber: pageNumber,
      currentPageSize: pageSize,
    };
  }

  public async getActiveSubscriptions({
    userId,
  }: {
    userId: number;
  }): Promise<{ id: number; pendingProfileViews: number }[]> {
    const subQuery = this.em
      .getKnex()
      .select(
        'sp.name as subscriptionName',
        'es.id',
        'es.created_at as createdAt',
        'es.expiry_date as expiryDate',
        'sp.valid_for_days as validForDays',
        this.em.getKnex().raw('\'success\' as "paymentStatus"'),
        this.em
          .getKnex()
          .raw(
            `(Select count(DISTINCT (epv.job_seeker_user_id)) from employer_profile_views epv where epv.employer_subscription_id = cast(es.id as integer)) as "viewCounts"`,
          ),
        this.em
          .getKnex()
          .raw(
            `(es.allowed_profile_view_count - (Select count(DISTINCT (epv.employer_user_id, epv.job_seeker_user_id, epv.employer_subscription_id)) from employer_profile_views epv where epv.employer_subscription_id = cast(es.id as integer))) as "pendingProfileViews"`,
          ),
      )
      .from('job_seeker_subscription_plan as sp')
      .join('employer_job_seeker_subscription as es', 'sp.id', 'es.plan_id')
      .where('es.expiry_date', '>', this.em.getKnex().fn.now())
      .where('es.employer_user_id', userId)
      .as('sub');

    const activeSubscriptions = await this.em
      .getKnex()
      .select('*')
      .from(subQuery)
      .where('pendingProfileViews', '>', 0)
      .orderBy('expiryDate', 'asc');

    return activeSubscriptions;
  }

  public async filterJobSeekerDirectory(
    props: SubscriptionShapes['filterDirectory']['query'],
    { employer }: { employer: Employer },
  ) {
    const {
      locationIds,
      skillIds,
      preferredLocations,
      searchText,
      pageSize,
      pageNumber,
      maxCTC,
      minCTC,
      isPortfolioAvailable,
      experience,
      noticePeriod,
      lastLoginFromValue,
      lastLoginToValue,
      languageIds,
    } = props;

    const nonExpiredSubscription = await this.em.findOne(
      EmployerJobSeekerSubscription,
      {
        employer: employer,
        expiryDate: { $gte: new Date() },
      },
    );

    if (!nonExpiredSubscription) {
      throw new ForbiddenException('No such subscription found');
    }

    let qb = this.em
      .getKnex()('user as u')
      .select(
        'ct.name as city',
        'u.id as id',
        'u.picture',
        'j.headline',
        this.em.getKnex().raw('j.experience_in_year as "experienceInYears"'),
        this.em
          .getKnex()
          .raw('j.expected_salary_in_lpa as "expectedSalaryInLpa"'),
        this.em
          .getKnex()
          .raw('concat(u.first_name, \' \', u.last_name) as "fullName"'),
        this.em
          .getKnex()
          .raw(
            '(SELECT ARRAY_AGG(s.name) FROM skill s, job_seeker_skills js WHERE js.job_seeker_user_id = u.id AND s.id = js.skill_id) AS skills',
          ),
        this.em
          .getKnex()
          .raw(
            `(EXISTS (SELECT 1 FROM employer_profile_views epv WHERE epv.employer_user_id = ? AND epv.job_seeker_user_id = u.id)) AS "hasViewed"`,
            [employer.user.id],
          ),
        this.em.getKnex().raw(
          `
            (SELECT 
                jssp.name
            FROM 
                employer_profile_views AS epv,
                employer_job_seeker_subscription AS ejss,
                job_seeker_subscription_plan AS jssp
            WHERE 
                jssp.id = ejss.plan_id
                AND epv.employer_subscription_id = ejss.id
                AND ejss.expiry_date > NOW()
                AND ejss.employer_user_id = ?
                AND epv.job_seeker_user_id = u.id
            GROUP BY 
                jssp.name
            LIMIT 1    
                ) AS "viewedUnderPlan"
        `,
          [employer.user.id],
        ),
      )
      .leftJoin('job_seeker as j', 'j.user_id', 'u.id')
      .leftJoin('city as ct', 'ct.id', 'j.city_id')
      .where('u.role', UserRole.jobSeeker);

    if (maxCTC) {
      qb = qb.andWhere(function () {
        this.where('j.expected_salary_in_lpa', '<=', maxCTC);
      });
    }

    if (minCTC) {
      qb = qb.where(function () {
        this.where('j.expected_salary_in_lpa', '>=', minCTC);
      });
    }

    if (!isNil(experience)) {
      const [comp, val] = getExperienceFilterForJobSeeker(experience);
      qb = qb.andWhere('j.experience_in_year', comp, val);
    }

    if (isPortfolioAvailable) {
      qb = qb.andWhere('j.social_media_link', '<>', null);
    }

    if (lastLoginToValue) {
      qb = qb.whereExists(function () {
        this.select('*')
          .from('analytic_event as ae')
          .whereRaw('user_id = u.id')
          .where('ae.created_at', '<=', lastLoginToValue);
      });
    }

    if (lastLoginFromValue) {
      qb = qb.whereExists(function () {
        this.select('*')
          .from('analytic_event as ae')
          .whereRaw('user_id = u.id')
          .where('ae.created_at', '>=', lastLoginFromValue);
      });
    }

    if (noticePeriod && noticePeriod.length > 0) {
      qb = qb.whereExists(function () {
        this.select('*')
          .from('job_seeker')
          .whereIn('notice_period', noticePeriod)
          .whereRaw('user_id = u.id');
      });
    }

    if (skillIds && skillIds.length > 0) {
      qb = qb.whereExists(function () {
        this.select('*')
          .from('job_seeker_skills')
          .whereIn('skill_id', skillIds)
          .whereRaw('job_seeker_user_id = u.id');
      });
    }
    if (preferredLocations && preferredLocations.length > 0) {
      qb = qb.whereExists(function () {
        this.select('*')
          .from('job_seeker_preferred_locations as jspl')
          .whereIn('city_id', preferredLocations)
          .whereRaw('job_seeker_user_id = u.id');
      });
    }

    if (locationIds && locationIds.length > 0) {
      qb = qb.whereExists(function () {
        this.select('*')
          .from('job_seeker')
          .whereIn('city_id', locationIds)
          .whereRaw('user_id = u.id');
      });
    }

    if (languageIds && languageIds.length > 0) {
      qb = qb.whereExists(function () {
        this.select('*')
          .from('job_seeker')
          .join(
            'job_seeker_languages',
            'job_seeker.user_id',
            '=',
            'job_seeker_languages.job_seeker_user_id',
          )
          .whereIn('job_seeker_languages.language_id', languageIds)
          .whereRaw('user_id = u.id');
      });
    }

    if (searchText) {
      qb = qb.andWhere(function () {
        this.orWhere('j.headline', 'ilike', `%${searchText}%`)
          .orWhere('u.first_name', 'ilike', `%${searchText}%`)
          .orWhere('u.last_name', 'ilike', `%${searchText}%`)
          .orWhere('ct.name', 'ilike', `%${searchText}%`)
          .orWhere('j.profile_summary', 'ilike', `%${searchText}%`);
      });
    }

    if (isNumber(searchText)) {
      qb = qb.orWhere('j.experience_in_year', '=', `${searchText}`);
    }

    const totalItemsPromise = qb
      .clone()
      .clearOrder()
      .clearSelect()
      .count('* as count')
      .first();

    const usersPromise = qb.limit(pageSize).offset(pageSize * (pageNumber - 1));

    const [totalItems, users] = await Promise.all([
      totalItemsPromise,
      usersPromise,
    ]);

    return {
      results: users,
      currentPageSize: pageSize,
      currentPageNumber: pageNumber,
      totalItems: totalItems?.count,
      totalPages: totalItems
        ? Math.ceil((totalItems.count as number) / pageSize)
        : 1,
    };
  }

  public async createBulkEmployerProfileView({
    jobSeekers,
    subscriptionId,
    employer,
  }: {
    jobSeekers: JobSeeker[];
    subscriptionId: number;
    employer: Employer;
  }) {
    const storedEmployerProfileViews = await this.em.find(
      EmployerProfileViews,
      {
        employer: employer,
        jobSeeker: {
          user: { $in: jobSeekers.map((js) => js.user) },
        },
        employerSubscription: {
          employer,
          expiryDate: { $gte: new Date() },
        },
      },
      {
        populate: ['jobSeeker'],
      },
    );

    const subscriptionProfileViews = await this.em.find(EmployerProfileViews, {
      employerSubscription: { id: subscriptionId },
    });

    let newViews: EmployerProfileViews[] = [];

    await this.em.transactional(async (em) => {
      const [subDetail] = await Promise.all([
        em.findOneOrFail(
          EmployerJobSeekerSubscription,
          {
            id: subscriptionId,
          },
          {
            lockMode: LockMode.PESSIMISTIC_WRITE,
          },
        ),
      ]);

      const nonViewedJobSeekers = jobSeekers.filter(
        (j) =>
          !storedEmployerProfileViews.find(
            (v) => v.jobSeeker.user.id === j.user.id,
          ),
      );

      if (
        subDetail.allowedProfileViewCount <
        subscriptionProfileViews.length + nonViewedJobSeekers.length
      ) {
        throw new ForbiddenException(
          'Invite count has exceeded from your current plan',
        );
      }

      newViews = nonViewedJobSeekers.map((jobSeeker) => {
        return new EmployerProfileViews({
          employer,
          jobSeeker: jobSeeker,
          employerSubscription: subDetail,
          jobSeekerData: JSON.parse(JSON.stringify(jobSeeker)) as JSON,
        });
      });

      await em.persistAndFlush(newViews);
    });

    return [...storedEmployerProfileViews, ...newViews];
  }

  public async createEmployerProfileView({
    jobSeekerId,
    subscriptionId,
    employer,
  }: {
    jobSeekerId: number;
    subscriptionId: number;
    employer: Employer;
  }) {
    const userObj = await this.em.findOne(
      User,
      { id: jobSeekerId },
      { populate: ['jobSeeker'] },
    );

    if (!userObj?.jobSeeker) {
      throw new BadRequestException('JobSeeker not found');
    }

    const views = await this.createBulkEmployerProfileView({
      jobSeekers: [userObj.jobSeeker],
      subscriptionId,
      employer,
    });

    if (views.length === 0) {
      throw new ForbiddenException('No more profile views left');
    }

    return { viewId: views[0].id };
  }

  public async getAllEmployerCreatedJobs(
    query: SubscriptionShapes['getAllJobsFromEmployer']['query'],
    { employer }: { employer: Employer },
  ) {
    const conditions: FilterQuery<Job> = {
      createdByEmployer: employer,
      isPosted: true,
      isAdminApproved: true,
    };

    if (query.searchText) {
      conditions.searchVector = { $ilike: `%${query.searchText}%` };
    }

    const [jobs, count] = await this.em.findAndCount(Job, conditions, {
      offset: (query.pageNumber - 1) * query.pageSize,
      limit: query.pageSize,
      orderBy: [{ createdAt: QueryOrder.DESC }, { id: QueryOrder.DESC }],
      populate: ['cities', 'company'],
    });

    return createPaginatedResponse({
      results: jobs,
      pageNumber: query.pageNumber,
      pageSize: query.pageSize,
      totalCount: count,
    });
  }

  public async getJobSeekerProfile({
    employerProfileViewId,
    employer,
  }: {
    employerProfileViewId: string;
    employer: Employer;
  }) {
    const employerProfileView = await this.em.findOneOrFail(
      EmployerProfileViews,
      {
        id: employerProfileViewId,
        employer,
      },
      { populate: ['jobSeeker', 'employerSubscription.expiryDate'] },
    );

    if (employerProfileView.employerSubscription.expiryDate < new Date()) {
      throw new ForbiddenException('Subscription has expired');
    }

    const u = await this.em.findOneOrFail(
      User,
      {
        jobSeeker: employerProfileView.jobSeeker,
      },
      {
        populate: [
          'jobSeeker',
          'jobSeeker.city',
          'jobSeeker.subfunction',
          'jobSeeker.preferredLocations',
          'jobSeeker.languages',
          'jobSeeker.skills',
        ],
      },
    );

    if (!u.jobSeeker) {
      throw new BadRequestException('JobSeeker not found');
    }

    return {
      userId: u.id,
      firstName: u.firstName,
      lastName: u.lastName,
      email: u.email,
      phone: u.phone,
      role: u.role,
      picture: u.picture,
      profileSummary: u.jobSeeker.profileSummary,
      headline: u.jobSeeker.headline,
      expectedSalaryInLpa: u.jobSeeker.expectedSalaryInLpa,
      resume: u.jobSeeker.resume,
      preferredLocations: u.jobSeeker.preferredLocations.$.getItems().map(
        (c) => ({ id: c.id, name: c.name }),
      ),
      languages: u.jobSeeker.languages.getItems().map((c) => ({
        id: c.id,
        name: c.name,
      })),
      experienceInYear: u.jobSeeker.experienceInYear,
      city: {
        id: u.jobSeeker.city.id,
        name: u.jobSeeker.city.name,
      },
      noticePeriod: u.jobSeeker.noticePeriod,
      workSchedule: u.jobSeeker.workSchedule,
      skills: u.jobSeeker.skills.getItems().map((s) => ({
        id: s.id,
        name: s.name,
      })),
    };
  }

  public async getLastPaymentDetails({ employer }: { employer: Employer }) {
    const lastPaymentDetails = await this.em.findOne(
      JobSeekerSubscriptionOrder,
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

  public async suggestionActiveSubscriptions({
    employer,
  }: {
    employer: Employer;
  }) {
    const activeSubscriptions = await this.em.find(
      EmployerJobSeekerSubscription,
      {
        employer: employer,
        expiryDate: { $gte: new Date() },
      },
      {
        populate: ['plan'],
        fields: ['id', 'expiryDate'],
        orderBy: { expiryDate: QueryOrder.ASC, plan: { name: QueryOrder.ASC } },
      },
    );

    return activeSubscriptions.map((s) => {
      return {
        id: s.id,
        name: s.plan.name,
        expiryDate: s.expiryDate,
      };
    });
  }

  @Schedule('updatePaymentStatus', CronExpressions.everyFiveMinutes)
  async updatePaymentStatus() {
    await this.subscriptionBaseService.updateSubscriptions(
      JobSeekerSubscriptionOrder,
      (order) => {
        const subscription = new EmployerJobSeekerSubscription({ order });
        this.em.persist(subscription);
      },
    );
  }

  public async getViewedUsersForSubscription(
    props: SubscriptionShapes['getViewedUsersForSubscription']['query'],
  ) {
    const { pageNumber, pageSize, subscriptionId } = props;

    const [profileViews, count] = await this.em.findAndCount(
      EmployerProfileViews,
      {
        employerSubscription: this.em.getReference(
          EmployerJobSeekerSubscription,
          subscriptionId,
        ),
      },
      {
        fields: ['jobSeeker.user.fullName', 'createdAt'],
        limit: pageSize,
        offset: (pageNumber - 1) * pageSize,
        populate: ['employerSubscription', 'jobSeeker.user'],
      },
    );

    return createPaginatedResponse({
      results: profileViews.map((item) => ({
        name: item.jobSeeker.user.fullName,
        viewDate: item.createdAt,
        expiryDate: item.employerSubscription.expiryDate,
      })),
      pageNumber,
      pageSize,
      totalCount: count,
    });
  }

  @QueueMessageHandler(QueueNamesEnum.jobInvite)
  async sendJobInviteEmail({
    data,
  }: QueuePayloadData<QueueNamesEnum.jobInvite>) {
    const { inviteId } = data;

    const invite = await this.em.findOneOrFail(
      EmployerJobSeekerInvite,
      {
        id: inviteId,
      },
      {
        populate: ['jobSeeker.user', 'job', 'job.cities', 'job.company'],
      },
    );

    const msg = render(
      inviteEmail({
        name: invite.jobSeeker.user.fullName,
        companyName: invite.job.company
          ? invite.job.company.name
          : invite.job.companyName,
        applyNowUrl:
          generateJobIdPageUrl(this.frontEndUrl as string, invite.job) +
          `?inviteId=${invite.id}`,
      }),
    );

    await this.emailService.sendEmail(
      msg,
      invite.jobSeeker.user.fullName,
      invite.jobSeeker.user.email,
    );

    wrap(invite).assign({
      emailStatus: EmailStatusEnum.Success,
    });
  }
}
