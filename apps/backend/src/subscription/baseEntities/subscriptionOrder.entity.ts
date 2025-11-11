import { Enum, ManyToOne, PrimaryKey, Property } from '@mikro-orm/core';
import { PaymentStatusEnum } from '../../../../contract/enum';
import { Employer } from 'src/user/entities/employer.entity';

interface commonOrderProps {
  employer: Employer;
  invoiceLink: string;
  razorpayOrderId: string;
  razorpayInvoiceId: string;
  paidAmount: number;
  validForDays: number;
}

export abstract class CommonOrder {
  @PrimaryKey()
  id!: number;

  @Property()
  createdAt = new Date();

  @Property({ onUpdate: () => new Date() })
  updatedAt = new Date();

  @Property()
  validForDays: number;

  @ManyToOne(() => Employer)
  employer: Employer;

  @Property()
  paymentMethod: string = '';

  @Enum({ items: () => PaymentStatusEnum })
  paymentStatus: PaymentStatusEnum;

  @Property()
  paidAmount: number;

  @Property()
  razorpayOrderId: string;

  @Property()
  razorpayPaymentId: string | null | undefined;

  @Property()
  razorpayInvoiceId: string;

  @Property()
  invoiceLink: string | null;

  constructor({
    employer,
    invoiceLink,
    razorpayOrderId,
    razorpayInvoiceId,
    paidAmount,
    validForDays,
  }: commonOrderProps) {
    this.createdAt = new Date();
    this.employer = employer;
    this.razorpayInvoiceId = razorpayInvoiceId;
    this.razorpayPaymentId = null;
    this.razorpayOrderId = razorpayOrderId;
    this.paidAmount = paidAmount;
    this.paymentStatus = PaymentStatusEnum.pending;
    this.validForDays = validForDays;
    this.invoiceLink = invoiceLink;
  }
}

export abstract class CommonSubscriptionOrder extends CommonOrder {
  @Property()
  validForDays: number;

  constructor(props: { validForDays: number } & commonOrderProps) {
    super(props);
    this.validForDays = props.validForDays;
  }
}
