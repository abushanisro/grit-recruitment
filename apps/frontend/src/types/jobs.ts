import { ClientInferResponseBody } from "@ts-rest/core";
import { contract } from "contract";
import {
  addJobDto,
  commonInfo,
  manageJobApplicationsOfJobseekersResponse,
} from "contract/job/types";
import { CustomSelectOption } from "./form";
import { infer as zodInfer } from "zod/lib/types";

export type JobPaginatedListType = ClientInferResponseBody<
  typeof contract.job.filter,
  200
>;

export type SavedJobPaginatedListType = ClientInferResponseBody<
  typeof contract.job.fetchSavedJobs,
  200
>;
export type AppliedJobPaginatedListType = ClientInferResponseBody<
  typeof contract.job.getAppliedJobs,
  200
>;

export type ManageJobsPaginatedListType = ClientInferResponseBody<
  typeof contract.job.fetchJobsByEmployer,
  200
>;

export type ManageJobseekerJobsPaginatedListType = ClientInferResponseBody<
  typeof contract.job.manageJobApplicationsOfJobseekers,
  200
>;

export type JobSeekerApplicantsResponse = zodInfer<
  typeof manageJobApplicationsOfJobseekersResponse
>;

export type JobIdResultsType = ClientInferResponseBody<
  typeof contract.job.getJobById,
  200
>;

export type CreateJobType = zodInfer<typeof addJobDto>;

export type ModifiedCreateJobType = Omit<
  CreateJobType,
  | "title"
  | "companyId"
  | "employmentTypes"
  | "employmentModes"
  | "industryIds"
  | "minExperienceInYears"
  | "maxExperienceInYears"
  | "subfunctionIds"
  | "cityIds"
  | "isBoosted"
  | "subscriptionId"
> & {
  title: string | null;
  companyId: null | number;
  companyName: string;
  employmentTypes: CustomSelectOption[] | null;
  employmentModes: CustomSelectOption[] | null;
  minExperienceInYears: null | number;
  maxExperienceInYears: null | number;
  industryIds: CustomSelectOption[] | null;
  subfunctionIds: CustomSelectOption[] | null;
  cityIds: CustomSelectOption[] | null;
  isBoosted: boolean;
  subscriptionId: string | null;
};

export interface ManageJobApplicantCountInterface {
  pending: number;
  shortlist: number;
  reject: number;
}
export type CommonInfoType = zodInfer<typeof commonInfo>;

export interface FilterInterface {
  searchText: string;
  location: CustomSelectOption[] | null;
  employmentType: CustomSelectOption[] | null;
  salary: CustomSelectOption | null;
  workSchedule: CustomSelectOption[] | null;
  industry: CustomSelectOption[] | null;
  subFunctions: CustomSelectOption[] | null;
  pageSize: number;
  pageNumber: number;
  experience: CustomSelectOption | null;
}
