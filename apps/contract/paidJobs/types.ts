import { PaymentStatusEnum } from "contract/enum";
import { z } from "zod";
export const subscriptionPlansResponse = z.object({
  id: z.number(),
  price: z.number(),
  name: z.string(),
  validForDays: z.number(),
  boostLimit: z.number(),
  boostDays: z.number(),
  description: z.string(),
  points: z.number(),
});

export const getOrderResponse = z.object({
  orderId: z.string(),
});
export const getOrderBody = z.object({
  subscriptionPlanId: z.string(),
});
export const updateSubscriptionBody = z.object({
  razorpay_invoice_id: z.string(),
  razorpay_payment_id: z.string(),
  razorpay_signature: z.string(),
  razorpay_invoice_status: z.string(),
  razorpay_invoice_receipt: z.string(),
});
export const subscriptionHistoryResponse = z.object({
  id: z.number(),
  subscriptionName: z.string(),
  createdAt: z.string(),
  paidAmount: z.number(),
  paymentMethod: z.string(),
  paymentStatus: z.string(),
  invoiceLink: z.string(),
  boostLimit: z.number(),
  boostDays: z.number(),
  validTill: z.string(),
  validForDays: z.number(),
});
export const boostJobBody = z.object({
  jobId: z.string().transform(Number),
  subscriptionId: z.string().transform(Number),
});
export const activeSubscriptionsResponse = z.object({
  id: z.string(),
  subscriptionName: z.string(),
  createdAt: z.string(),
  expiryDate: z.string(),
  totalBoosts: z.number(),
  boostLimit: z.number(),
  boostDays: z.number(),
  boostUsed: z.number(),

  validTill: z.string(),
  validForDays: z.number(),
  paymentStatus: z.nativeEnum(PaymentStatusEnum),
});

export const getBoostedJobsForSubscriptionResponse = z.object({
  title: z.string(),
  boostEndDate: z.string(),
  boostStartDate: z.string(),
});
export const getLastPaymentDetailsResponse = z.object({
  reference_no: z.string(),
  payment_date: z.string(),
  payment_method: z.string(),
  subscription_validity: z.number(),
  invoice_link: z.string(),
  amount: z.number(),
});
