import {
  EmploymentMode,
  EmploymentType,
  JobApplicationStatus,
  NoticePeriodEnum,
  UserRole,
} from "contract/enum";
import { z } from "zod";

export const jobSearchDto = z.object({
  pageSize: z.string().transform(Number),
  pageNumber: z.string().transform(Number),
  cityIds: z.string().transform(Number).array().optional(),
  minSalaryInLpa: z.string().transform(Number).optional(),
  maxSalaryInLpa: z.string().transform(Number).optional(),
  subfunctionIds: z.string().transform(Number).array().optional(),
  searchText: z.string().optional(),
  employmentType: z.nativeEnum(EmploymentType).array().optional(),
  employmentMode: z.nativeEnum(EmploymentMode).array().optional(),
  minExperienceInYear: z.string().transform(Number).optional(),
  maxExperienceInYear: z.string().transform(Number).optional(),
  industry: z.string().transform(Number).array().optional(),
});

export const commonInfo = z.object({ id: z.number(), name: z.string() });

export const jobFilterResponse = z.object({
  id: z.number(),
  title: z.string(),
  types: z.nativeEnum(EmploymentType).array(),
  modes: z.nativeEnum(EmploymentMode).array(),
  minExp: z.number(),
  maxExp: z.number(),
  companyName: z.string(),
  companyLogo: z.string().nullable(),
  minCtc: z.number().nullable(),
  maxCtc: z.number().nullable(),
  cities: commonInfo.array(),
  industries: commonInfo.array(),
  subFunctions: commonInfo.array(),
  isExternalJob: z.boolean(),
  externalLink: z.string().nullable(),
  createdAt: z.date(),
  isSaved: z.boolean(),
  adminApprovedTime: z.date().nullable(),
});

export const jobIdResponse = z.object({
  id: z.number(),
  designation: z.string(),
  companyName: z.string(),
  companyLogo: z.string().nullable(),
  locations: commonInfo.array(),
  industries: commonInfo.array(),
  subFunctions: commonInfo.array(),
  employmentModes: z.nativeEnum(EmploymentMode).array(),
  employmentTypes: z.nativeEnum(EmploymentType).array(),
  description: z.object({}).nullable(),
  minSalaryInlpa: z.number().nullable(),
  maxSalaryInLpa: z.number().nullable(),
  minExperienceInYears: z.number(),
  maxExperienceInYears: z.number(),
  createdAt: z.date(),
  isExternalJob: z.boolean(),
  externalLink: z.string().nullable(),
  isSaved: z.boolean(),
  isApplied: z.boolean(),
  adminApprovedTime: z.date().nullable(),
});

export const jobViewDto = z.object({
  jobId: z.string().transform(Number),
});

export const paginationDto = z.object({
  pageSize: z.string().transform(Number),
  pageNumber: z.string().transform(Number),
});

export const fetchJobsByEmployerJobSeekerDto = z.object({
  pageSize: z.string().transform(Number),
  pageNumber: z.string().transform(Number),
});

export const fetchAppliedJobResponse = z.object({
  id: z.number(),
  title: z.string(),
  companyLogo: z.string().nullable(),
  employmentTypes: z.nativeEnum(EmploymentType).array(),
  employmentModes: z.nativeEnum(EmploymentMode).array(),
  minExp: z.number(),
  maxExp: z.number(),
  companyName: z.string(),
  minCtc: z.number().nullable(),
  maxCtc: z.number().nullable(),
  cities: commonInfo.array(),
  industries: commonInfo.array(),
  subFunctions: commonInfo.array(),
  status: z.nativeEnum(JobApplicationStatus),
  jobAppliedTime: z.date(),
  statusChangedTime: z.date().nullable(),
  isExternalJob: z.boolean(),
  externalLink: z.string().nullable(),
});

export const addJobDto = z.object({
  title: z.string().min(1).max(150),
  companyId: z.number(),
  cityIds: z.number().array(),
  languageIds: z.number().array(),
  industryIds: z.number().array(),
  subfunctionIds: z.number().array(),
  description: z.string().refine((value) => {
    try {
      JSON.parse(value);
      return true;
    } catch {
      return false;
    }
  }, "Must be a valid JSON string"),
  minSalaryInLpa: z.number().optional(),
  maxSalaryInLpa: z.number().optional(),
  employmentTypes: z.nativeEnum(EmploymentType).array(),
  employmentModes: z.nativeEnum(EmploymentMode).array(),
  minExperienceInYears: z.number(),
  maxExperienceInYears: z.number(),
  isPosted: z.boolean(),
  isPremium: z.boolean().default(false),
  subscriptionId: z.string().transform(Number).optional(),
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
  expectedSalaryInLpa: z.number().nullable(),
  resume: z.string().optional(),
  videoResume: z.string(),
  preferredLocations: commonInfo.array(),
  languages: commonInfo.array(),
  experienceInYear: z.number(),
  noticePeriod: z.nativeEnum(NoticePeriodEnum),
  workSchedule: z.nativeEnum(EmploymentMode).nullable(),
});

export const changeApplicationStatusDto = z.object({
  jobId: z.number(),
  applicantUserId: z.number(),
  changedStatus: z.nativeEnum(JobApplicationStatus),
});
export const fetchJobsResponse = z.object({
  id: z.number(),
  title: z.string(),
  employmentTypes: z.nativeEnum(EmploymentType).array(),
  employmentModes: z.nativeEnum(EmploymentMode).array(),
  minExp: z.number(),
  maxExp: z.number(),
  companyName: z.string(),
  companyLogo: z.string().nullable(),
  minCtc: z.number().nullable(),
  maxCtc: z.number().nullable(),
  cities: commonInfo.array(),
  industries: commonInfo.array(),
  subFunctions: commonInfo.array(),
  createdAt: z.date(),
  isExternalJob: z.boolean(),
  externalLink: z.string().nullable(),
  isPosted: z.boolean(),
  isAdminApproved: z.boolean(),
  adminApprovedTime: z.date().nullable(),
  postedTime: z.date().nullable(),
  isBoosted: z.boolean(),
  boostedDaysLeft: z.number(),
});

export const jobSeekerJobsResponse = z.object({
  id: z.number(),
  title: z.string(),
  employmentTypes: z.nativeEnum(EmploymentType).array(),
  employmentModes: z.nativeEnum(EmploymentMode).array(),
  minExp: z.number(),
  maxExp: z.number(),
  companyName: z.string(),
  minCtc: z.number().nullable(),
  maxCtc: z.number().nullable(),
  cities: commonInfo.array(),
  industries: commonInfo.array(),
  subFunctions: commonInfo.array(),
  companyLogo: z.string().nullable(),
  jobSavedTime: z.date(),
  isExternalJob: z.boolean(),
  externalLink: z.string().nullable(),
});

export const createCompanyDto = z.object({
  name: z.string(),
  website: z.string(),
  state: z.string(),
  cityIds: z.number().array(),
  industryIds: z.number().array(),
  logo: z.string(),
});

export const applyJobDto = z.object({
  jobId: z.number(),
  areYouOkayWithTheLocation: z.boolean(),
  coverLetter: z.string().min(0).max(2000).nullable(),
});
export const manageJobApplicationsOfJobseekersResponse = z.object({
  jobApplicationId: z.number(),
  jobSeekerId: z.number(),
  jobSeekerProfileImage: z.string().nullable(),
  jobSeekerDescription: z.string().nullable(),
  jobId: z.number(),
  firstName: z.string(),
  lastName: z.string(),
  experience: z.number(),
  location: z.string(),
  expectedSalaryInLpa: z.number().nullable(),
  noticePeriod: z.nativeEnum(NoticePeriodEnum),
  resume: z.string(),
  videoResume: z.string(),
  lastLoginTime: z.string().nullable(),
  status: z.nativeEnum(JobApplicationStatus),
  adminApprovedTime: z.date().nullable(),
  coverLetter: z.string().nullable(),
});

export const manageJobApplicationsOfJobseekersDto = z.object({
  pageSize: z.string().transform(Number),
  pageNumber: z.string().transform(Number),
  status: z.nativeEnum(JobApplicationStatus),
  jobId: z.string().transform(Number),
  noticePeriod: z.nativeEnum(NoticePeriodEnum).array().optional(),
  locationIds: z.string().transform(Number).array().optional(),
  languageIds: z.string().transform(Number).array().optional(),
  skillIds: z.string().transform(Number).array().optional(),
  minExperienceInYears: z.string().transform(Number).optional(),
  maxExperienceInYears: z.string().transform(Number).optional(),
  minSalaryInLpa: z.string().transform(Number).optional(),
  maxSalaryInLpa: z.string().transform(Number).optional(),
  jobSeekerName: z.string().optional(),
});

export const getSuggestionJobTitleInManageApplicantsJobseekerResponse = z
  .object({
    jobId: z.number(),
    jobTitle: z.string(),
    adminApprovedTime: z.date().nullable(),
  })
  .array();

export const errorMessageResponse = z.object({
  error: z.string(),
  message: z.string(),
  statusCode: z.number(),
});

const EmploymentTypeSchema = z.nativeEnum(EmploymentType);
const EmploymentModeSchema = z.nativeEnum(EmploymentMode);

export const bulkUploadJobsForJobSeekerDto = z.object({
  jobType: z.literal(UserRole.jobSeeker),
  jobTitle: z.string().min(1).max(150),
  description: z.string().min(10).max(2000).optional(),
  companyName: z.string().min(3).max(200),
  minSalaryInLpa: z
    .string()
    .optional()
    .nullable()
    .refine(
      (value) =>
        value === null ||
        value === undefined ||
        (!isNaN(Number(value)) && Number(value) >= 1 && Number(value) <= 50),
      {
        message: "Value must be between 1 and 50",
      }
    )
    .transform(Number),
  maxSalaryInLpa: z
    .string()
    .optional()
    .nullable()
    .refine(
      (value) =>
        value === null ||
        value === undefined ||
        (!isNaN(Number(value)) && Number(value) >= 1 && Number(value) <= 50),
      {
        message: "Value must be between 1 and 50",
      }
    )
    .transform(Number),
  minExperienceInYears: z
    .string()
    .refine(
      (value) => {
        const num = Number(value);
        return !isNaN(num) && num >= 0 && num <= 50;
      },
      {
        message: "Value must be a number between 0 and 50",
      }
    )
    .transform(Number),
  maxExperienceInYears: z
    .string()
    .refine(
      (value) => {
        const num = Number(value);
        return !isNaN(num) && num >= 0 && num <= 50;
      },
      {
        message: "Value must be a number between 0 and 50",
      }
    )
    .transform(Number),
  location: z.string().transform((s) => s.split(",").map(String)),
  typeOfEmployment: z
    .string()
    .transform((s) => s.split(",").map((s) => s.trim()))
    .refine(
      (types) => types.every((t) => EmploymentTypeSchema.safeParse(t).success),
      (t) => ({
        message:
          `Some types are not valid. Received ${t}. Supported types are ` +
          Object.values(EmploymentType).join(", "),
      })
    )
    .transform((types) =>
      types.map((type) => {
        const key = Object.keys(EmploymentType).find(
          (key) => EmploymentType[key as keyof typeof EmploymentType] === type
        );
        return EmploymentType[key as keyof typeof EmploymentType];
      })
    )
    .transform((types) => Array.from(new Set(types))), // Remove duplicates
  workSchedule: z
    .string()
    .transform((s) => s.split(",").map((s) => s.trim()))
    .refine(
      (types) => types.every((t) => EmploymentModeSchema.safeParse(t).success),
      (t) => ({
        message:
          `Some types are not valid. Received ${t}. Supported types are ` +
          Object.values(EmploymentMode).join(", "),
      })
    )
    .transform((types) =>
      types.map((type) => {
        const key = Object.keys(EmploymentMode).find(
          (key) => EmploymentMode[key as keyof typeof EmploymentMode] === type
        );
        return EmploymentMode[key as keyof typeof EmploymentMode];
      })
    )
    .transform((types) => Array.from(new Set(types))), // Remove duplicates
  industries: z.string().transform((s) => s.split(",").map(String)),
  subfunctions: z.string().transform((s) => s.split(",").map(String)),
  externalLink: z
    .string()
    .max(2000)
    .optional()
    .refine(
      (value) => {
        if (value === null || value === "" || value === undefined) {
          return true; // Allow null or empty string
        }
        try {
          new URL(value);
          return true; // Valid URL
        } catch (error) {
          return false; // Invalid URL
        }
      },
      {
        message:
          "Value must be empty for internal job or a valid URL for external job",
      }
    ),
});

export type BulkUploadJobsForJobSeekerDto = z.infer<
  typeof bulkUploadJobsForJobSeekerDto
>;
