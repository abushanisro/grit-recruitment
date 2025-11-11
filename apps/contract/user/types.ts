import {
  EmploymentMode,
  NoticePeriodEnum,
  UserRole,
  CompanySizeEnum,
} from "contract/enum";
import { commonDto } from "../common.dto";
import { z } from "zod";

export const commonInfo = z.object({ id: z.number(), name: z.string() });
export const jobSeekerResponse = commonDto.extend({
  id: z.number(),
  profileSummary: z.string(),
  headline: z.string(),
  expectedSalaryInLpa: z.number().nullable(),
  isPrivate: z.boolean(),
  externalLink: z.string().nullable(),
  resume: z.string(),
  videoResume: z.string(),
  preferredLocations: commonInfo.array(),
  experienceInYear: z.number(),
  languages: commonInfo.array(),
  subfunction: commonInfo.array(),
  city: commonInfo,
  noticePeriod: z.nativeEnum(NoticePeriodEnum),
  workSchedule: z.nativeEnum(EmploymentMode).nullable(),
  skills: commonInfo.array(),
  socialMediaLink: z.string().nullable(),
  isSubscribedToAlerts: z.boolean(),
});

export const companyResponse = z.object({
  name: z.string(),
  website: z.string().nullable(),
  industries: commonInfo.array(),
  logo: z.string().nullable(),
  companyId: z.number(),
});
export const employerResponse = commonDto.extend({
  employerId: z.number(),
  role: z.nativeEnum(UserRole),
  company: companyResponse,
  aboutCompany: z.string().min(100),
  companySize: z.nativeEnum(CompanySizeEnum).nullable(),
  verificationDocument: z.string(),
  city: commonInfo.nullable(),
  isVerified: z.boolean(),
  gstNumber: z.string(),
  gstAddress: z.string(),
});

export const jobSeekerDto = commonDto.extend({
  profileSummary: z.string().default(""),
  headline: z.string().min(10).max(150),
  expectedSalaryInLpa: z.number().nullable(),
  isPrivate: z.boolean(),
  externalLink: z.string().nullable(),
  resume: z.string(),
  videoResume: z.string(),
  preferredLocations: z.number().array(),
  experienceInYear: z.number(),
  languages: z.number().array().optional().default([]),
  subfunction: z.number().array(),
  city: z.number(),
  skills: z.number().array(),
  noticePeriod: z.nativeEnum(NoticePeriodEnum),
  workSchedule: z.nativeEnum(EmploymentMode).nullable(),
  socialMediaLink: z.string().nullable(),
  isSubscribedToAlerts: z.boolean().default(false),
});

export const updateCompany = z.object({
  name: z.string(),
  website: z.string().nullable(),
  industries: z.number().array(),
  logo: z.string().nullable(),
});

export const employerDto = commonDto.extend({
  role: z.literal(UserRole.employer),
  company: updateCompany,
  aboutCompany: z.string().min(5).max(2000).nullable().default(""),
  companySize: z.nativeEnum(CompanySizeEnum).nullable(),
  verificationDocument: z.string(),
  city: z.number().nullable(),
  gstNumber: z.string().min(3).max(500),
  gstAddress: z.string().min(3).max(500),
});

export const UserCreationSuccessSchema = z.object({
  isSuccess: z.boolean(),
  message: z.string(),
  token: z.string(),
});

export const UserProfileDto = commonDto.extend({
  role: z.nativeEnum(UserRole),
  isVerified: z.boolean(),
  hasHistoryOfPurchase: z.boolean().optional(),
  isPurchaseActive: z.boolean().optional(),
  isJobBoostPurchaseActive: z.boolean().optional(),
  companyName: z.string().optional(),
});

export type JobSeekerType = z.infer<typeof jobSeekerDto>;
export type EmployerType = z.infer<typeof employerDto>;
