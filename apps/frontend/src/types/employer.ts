import { ClientInferResponseBody } from "@ts-rest/core";
import { contract } from "contract";
import { infer as zodInfer } from "zod/lib/types";
import { employerDto } from "contract/user/types";
import { CustomSelectOption } from "@/types/form";
import { z } from "zod";
import {
  activeSubscriptionsResponse,
  filterDirectoryResponse,
  getAllJobsFromEmployerResponse,
  getLastPaymentDetailsResponse,
  subscriptionHistoryResponse,
  subscriptionPlansResponse,
  suggestionActiveSubscriptionsResponse,
} from "../../../contract/subscription/types";
import { jobSeekerProfileResponse } from "../../../contract/job/types";
import { TextProps } from "@mantine/core";

export type EmployerUserDataType = ClientInferResponseBody<
  typeof contract.user.getEmployerById,
  200
>;

export type EmployerUserCreationDataType = zodInfer<typeof employerDto>;

export type ModifiedEmployerUserCreationDataType = Omit<
  EmployerUserCreationDataType,
  "company" | "city" | "gstNumber" | "gstAddress"
> & {
  company: {
    name: string;
    website: string | null;
    logo: string | null;
    industries: CustomSelectOption[] | null;
  };
  city: CustomSelectOption | null;
  companySize: CustomSelectOption | null;
  gstNumber: string;
  gstAddress: string;
};

export type CreateEmployerType = ClientInferResponseBody<
  typeof contract.user.createEmployer,
  200
>;
export type UpdateEmployerType = ClientInferResponseBody<
  typeof contract.user.updateEmployer,
  200
>;

export type ViewJobseekerProfileFromManageApplicationType =
  ClientInferResponseBody<
    typeof contract.user.viewJobseekerProfileFromManageApplication,
    200
  >;

export type getAllJobsFromEmployerResponseType = z.infer<
  typeof getAllJobsFromEmployerResponse
>;
export type getSubscriptionHistoryType = z.infer<
  typeof subscriptionHistoryResponse
>;
export type jobseekerDirectoryType = z.infer<typeof filterDirectoryResponse>;
export type subscriptionPlansType = z.infer<typeof subscriptionPlansResponse>;
export type jobSeekerProfileType = z.infer<typeof jobSeekerProfileResponse>;
export type getActiveSubscriptionsType = z.infer<
  typeof activeSubscriptionsResponse
>;
export type suggestionActiveSubscriptionType = z.infer<
  typeof suggestionActiveSubscriptionsResponse
>;

export type getLastPaymentDetailsResponseType = z.infer<
  typeof getLastPaymentDetailsResponse
>;
export type getSubscriptionPlanType = ClientInferResponseBody<
  typeof contract.subscription.getSubscriptionPlans,
  200
>;

export interface FreelancerFilterRequest {
  pageNumber: number;
  status: string;
  freelancerJobId: number | null;
  expertiseLevel: CustomSelectOption[];
  locationIds: CustomSelectOption[];
  freelancerName: "";
}
export interface JobSeekerFilterRequest {
  pageNumber: number;
  status: string;
  // jobTitle: CustomSelectOption;
  noticePeriod: CustomSelectOption[];
  locationIds: CustomSelectOption[];
  experience: CustomSelectOption | null;
  ctc: CustomSelectOption | null;
  languageIds: CustomSelectOption[];
  skillIds: CustomSelectOption[];
  jobSeekerName: "";
}

export interface EmployerBannerDataInterface {
  location: string;
  name: string;
  title: string;
}

export interface FaqQuestionAnswerInterface {
  id: string;
  question: string;
  answer: string;
}

export interface postPaidResponseType {
  id: number;
  price: number;
  name: string;
  validForDays: number;
  boostLimit: number;
  description: string;
  points: number;
}

export interface SubscriptionPlan {
  id: number;
  name: string;
  description: string;
  allowedProfileCount: number;
  validForDays: number;
  price: number;
}

export interface PaginationInterface<T> {
  results: T[];
}

export interface MyPaymentHeading {
  id: number;
  subscriptionName: string;
  createdAt: string;
  paidAmount: number;
  paymentMethod: string;
  paymentStatus: string;
  invoiceLink: string;
}
export interface MyPaymentList {
  id: number;
  subscriptionName: string;
  createdAt: string;
  paidAmount: number;
  paymentMethod: string;
  paymentStatus: string;
  invoiceLink: string;
  allowedProfileCount: number;
  validTill: string;
  profileViewCount: number;
  validForDays: number;
}

export interface Date {
  date: string;
  time: string;
}

export interface MySubscriptionListInterface {
  id: number;
  subscriptionName: string;
  createdAt: string;
  expiryDate: string;
  pendingProfileView: number;
  viewCounts: number;
  paymentStatus: string;
  validForDays: number;
}
export interface MySubscriptionPostPaidList {
  id: string;
  subscriptionName: string;
  createdAt: string;
  expiryDate: string;
  paymentStatus: string;
  validForDays: number;
  boostLimit: number;
  boostUsed: number;
}
export interface MyBoostList {
  id: string;
  subscriptionName: string;
  createdAt: string;
  expiryDate: string;
  jobName: string;
  validForDays: number;
}

export interface Profile {
  pendingProfileView: number;
  viewCounts: number;
}

export type JobSeekerDirectoryPaginatedListType = ClientInferResponseBody<
  typeof contract.subscription.filterDirectory,
  200
>;

export interface ViewOrderDetails {
  name: string;
  cost: number;
  daysToComplete: number;
  revisionsAllowed: number;
  deliverableQuantity: number;
  deliverableUnit: string;
  description: string;
  faq: {
    question: string;
    answer: string;
  }[];
  questions: {
    question: string;
    questionId: number;
    answer?: string | undefined;
    fileLink?: string | undefined;
  }[];
}

export interface PackageCardDataType {
  name: string;
  quantity: number | null;
  unit: string;
  daysToComplete: number | null;
  revisionAllowed: number | null;
  cost: number | null;
}

export interface CustomTextProps extends TextProps {
  label?: string;
  children?: React.ReactNode;
}
