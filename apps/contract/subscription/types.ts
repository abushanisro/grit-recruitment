import {
  CompanySizeEnum,
  EmploymentMode,
  ExperienceInYear,
  NoticePeriodEnum,
  PaymentStatusEnum,
} from "contract/enum";
import { commonInfo } from "contract/user/types";
import { z } from "zod";

import { dateStringSchema, booleanStringSchema } from "../utils";

export const subscriptionPlansResponse = z.object({
  id: z.number(),
  name: z.string(),
  allowedProfileCount: z.number(),
  validForDays: z.number(),
  price: z.number(),
  description: z.string(),
});

export const getOrderResponse = z.object({
  orderId: z.string(),
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
  allowedProfileCount: z.number(),
  validTill: z.string(),
  profileViewCount: z.number(),
  validForDays: z.number(),
});

export const activeSubscriptionsResponse = z.object({
  id: z.number(),
  subscriptionName: z.string(),
  createdAt: z.string(),
  expiryDate: z.string(),
  validForDays: z.number(),
  pendingProfileView: z.number(),
  viewCounts: z.number(),
  paymentStatus: z.nativeEnum(PaymentStatusEnum),
});

export const filterDirectoryBody = z.object({
  skillIds: z.array(z.string().transform(Number)).optional(),
  locationIds: z.array(z.string()).optional(),
  preferredLocations: z.array(z.string()).optional(),
  searchText: z.string().optional(),
  noticePeriod: z.array(z.nativeEnum(NoticePeriodEnum)).optional(),
  lastLoginFromValue: dateStringSchema("lastLoginFromValue").optional(),
  lastLoginToValue: dateStringSchema("lastLoginToValue").optional(),
  minCTC: z.string().transform(Number).optional(),
  maxCTC: z.string().transform(Number).optional(),
  experience: z.nativeEnum(ExperienceInYear).optional(),
  languageIds: z.array(z.string()).optional(),
  isPortfolioAvailable: booleanStringSchema("isPortfolioAvailable").optional(),
});

export const filterDirectoryResponse = z.object({
  id: z.number(),
  headline: z.string(), // jobtitle as headline
  city: z.string(),
  picture: z.string(),
  skills: z.string().array(),
  experienceInYears: z.string(),
  hasViewed: z.boolean(),
  viewedUnderPlan: z.string(),
  fullName: z.string(),
  hasInvited: z.boolean(),
  expectedSalaryInLpa: z.number().nullable(),
});

export const getSubscriptionPlansQuery = z.object({
  profileCount: z.number().optional(),
});

export const getOrderBody = z.object({
  subscriptionPlanId: z.string(),
});

export const getJobSeekerProfileQuery = z.object({
  employerProfileViewId: z.string(),
});

export const jobSeekerProfileResponse = z.object({
  userId: z.number(),
  firstName: z.string(),
  lastName: z.string(),
  email: z.string(),
  phone: z.string(),
  role: z.string(),
  picture: z.string().nullable(),
  profileSummary: z.string(),
  headline: z.string(),
  city: commonInfo,
  skills: commonInfo.array(),
  expectedSalaryInLpa: z.number().nullable(),
  resume: z.string().optional(),
  preferredLocations: commonInfo.array(),
  languages: commonInfo.array(),
  experienceInYear: z.number(),
  noticePeriod: z.nativeEnum(NoticePeriodEnum),
  workSchedule: z.nativeEnum(EmploymentMode).nullable(),
});

export const getEmployerProfileViewResponse = z.object({
  viewId: z.string(),
});

export const getEmployerProfileViewBody = z.object({
  jobSeekerId: z.string().transform(Number),
  subscriptionId: z.string().transform(Number),
});

export const suggestionActiveSubscriptionsResponse = z
  .object({
    id: z.number(),
    name: z.string(),
    expiryDate: z.date(),
  })
  .array();

export const inviteUsersBody = z.object({
  jobId: z.number(),
  jobSeekerIds: z.number().array().min(1).max(20),
  subscriptionId: z.number(),
});

export const getAllJobsFromEmployerResponse = z.object({
  id: z.number(),
  createdAt: z.date(),
  title: z.string(),
  company: z.object({
    id: z.number(),
    createdAt: z.date(),
    updatedAt: z.date(),
    name: z.string(),
    website: z.string(),
    aboutCompany: z.string(),
    companySize: z.nativeEnum(CompanySizeEnum),
    logo: z.string(),
  }),
  employmentModes: z.nativeEnum(EmploymentMode),
  experienceInYears: z.number(),
  cities: commonInfo.array(),
  minSalaryInLpa: z.number(),
  maxSalaryInLpa: z.number(),
  minExperienceInYears: z.number(),
  maxExperienceInYears: z.number(),
});

export const viewJobSeekersProfileQuery = z.object({
  userId: z.string().transform(Number),
});

export const employerProfileViewBody = z.object({
  jobSeekerId: z.number(),
});

export const employerProfileViewResponse = z.object({
  id: z.string(),
});

export const getLastPaymentDetailsResponse = z.object({
  reference_no: z.string(),
  payment_date: z.string(),
  payment_method: z.string(),
  subscription_validity: z.number(),
  invoice_link: z.string(),
  amount: z.number(),
});

export const getViewedUsersForSubscriptionResponse = z.object({
  name: z.string(),
  viewDate: z.string(),
  expiryDate: z.string(),
});
