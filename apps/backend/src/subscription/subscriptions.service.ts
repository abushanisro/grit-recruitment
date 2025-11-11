import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { NestRequestShapes, nestControllerContract } from '@ts-rest/nest';
import Razorpay = require('razorpay');
import { subscriptionContract } from '../../../contract/subscription/contract';
import { render } from '@react-email/render';
import { EmailService } from 'src/auth/email.service';
import { paymentSuccessEmail } from './emails/paymentSuccess';
import { EntityClass, EntityName, FilterQuery } from '@mikro-orm/core';
import { EntityManager } from '@mikro-orm/postgresql';
import { Employer } from 'src/user/entities/employer.entity';
import { CommonSubscriptionPlan } from './baseEntities/subscriptionPlan.entity';
import { PaymentStatusEnum } from '../../../contract/enum';
import { CommonSubscriptionOrder } from './baseEntities/subscriptionOrder.entity';

export const subscriptionContractController =
  nestControllerContract(subscriptionContract);
export type SubscriptionShapes = NestRequestShapes<
  typeof subscriptionContractController
>;

@Injectable()
export class SubscriptionService {
  constructor(
    private readonly em: EntityManager,
    private configService: ConfigService,
    private emailService: EmailService,
  ) {}

  frontEndUrl = this.configService.get<string>('frontEndUrl');

  payInstance = new Razorpay({
    key_id: this.configService.get('razorpay.keyId') as string,
    key_secret: this.configService.get('razorpay.keySecret') as string,
  });

  public async getSubscriptionPlans<T>(entityClass: EntityClass<T>) {
    return this.em.find(entityClass, {});
  }

  public getActiveSubscriptionOptions<
    T extends { employer: Employer; expiryDate: Date },
  >(employer: Employer, options: FilterQuery<T> = {}): any {
    return {
      employer,
      expiryDate: { $gte: new Date() },
      ...options,
    };
  }

  public async createRazorpayOrderFromPlan<T extends CommonSubscriptionPlan>(
    planEntity: EntityName<T>,
    planId: string,
  ) {
    const subDetail = await this.em.findOneOrFail<T>(planEntity, {
      id: planId,
    } as any);

    const invoice = await this.payInstance.invoices.create({
      type: 'invoice',
      customer: {},
      line_items: [
        {
          name: subDetail.name,
          description: subDetail.description,
          amount: subDetail.price * 100,
          currency: 'INR',
          quantity: 1,
        },
      ],
      currency: 'INR',
    });

    return { subDetail, invoice };
  }

  public async sendSubscriptionEmail({
    email,
    transactionId,
    totalAmount,
    paymentDate,
    name,
    subscriptionType,
  }: {
    email: string;
    transactionId: string;
    paymentDate: Date;
    totalAmount: number;
    name: string;
    subscriptionType:
      | 'Paid Job Boost'
      | 'JobSeeker Directory'
      | 'Offer Package';
  }) {
    const msg = render(
      paymentSuccessEmail({
        name,
        transactionId,
        totalAmount,
        paymentDate,
        email,
        subscriptionType,
      }),
    );
    return this.emailService.sendEmail(msg, name, email);
  }

  public getPaymentDetails(payId: string) {
    return this.payInstance.payments.fetch(payId);
  }

  public getInvoiceDetails(invoiceId: string) {
    return this.payInstance.invoices.fetch(invoiceId);
  }

  public async updateSubscriptions<T extends CommonSubscriptionOrder>(
    orderEntityClass: EntityName<T>,
    createSubscriptionFromOrder: (order: T) => void,
  ) {
    const pendingOrders = (await this.em.find<T>(
      orderEntityClass as any,
      {
        paymentStatus: PaymentStatusEnum.pending,
      } as any,
      {
        limit: 50,
        populate: ['plan'] as any,
      },
    )) as T[];

    const delay = (ms: number) =>
      new Promise((resolve) => setTimeout(resolve, ms));

    await Promise.all(
      pendingOrders.map(async (order, index) => {
        await delay(index * 1000);
        const invoiceDetails = await this.getInvoiceDetails(
          order.razorpayInvoiceId,
        );

        if (invoiceDetails.status === 'paid' && invoiceDetails.payment_id) {
          const paymentDetails = await this.getPaymentDetails(
            invoiceDetails.payment_id,
          );

          await this.em.transactional(async (em) => {
            order.paymentStatus = PaymentStatusEnum.success;
            order.paymentMethod = paymentDetails.method;
            order.razorpayPaymentId = invoiceDetails.payment_id;

            createSubscriptionFromOrder(order);

            em.flush();
          });

          // await this.sendSubscriptionEmail({
          //   email: order.employer.user.email,
          //   transactionId: invoiceDetails.payment_id,
          //   paymentDate: new Date(),
          //   totalAmount: order.paidAmount,
          //   name: order.employer.user.fullName,
          //   subscriptionType: 'JobSeeker Directory',
          // });
        }
      }),
    );
  }
}
