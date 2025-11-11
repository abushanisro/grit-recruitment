import { ClientInferResponseBody } from "@ts-rest/core";
import { contract } from "contract";
import { infer as zodInfer } from "zod/lib/types";
import { jobSeekerDto } from "contract/user/types";
import { CustomSelectOption } from "@/types/form";
import { DateValue } from "@mantine/dates";

export type JobSeekerUserDataType = ClientInferResponseBody<
  typeof contract.user.getJobseekerProfile,
  200
>;

export type JobSeekerUserCreationDataType = zodInfer<typeof jobSeekerDto>;

export type ModifiedJobSeekerUserCreationDataType = Omit<
  JobSeekerUserCreationDataType,
  | "noticePeriod"
  | "city"
  | "experienceInYear"
  | "preferredLocations"
  | "languages"
  | "subfunction"
  | "skills"
> & {
  city: CustomSelectOption | null;
  noticePeriod: CustomSelectOption | null;
  experienceInYear: number | null;
  preferredLocations: CustomSelectOption[] | null;
  languages: CustomSelectOption[] | null;
  subfunction: CustomSelectOption[] | null;
  skills: CustomSelectOption[] | null;
};

export enum UserCreateModeEnum {
  create = "create",
  edit = "edit",
}
export type CreateJobSeekerType = ClientInferResponseBody<
  typeof contract.user.createJobseeker,
  200
>;
export type UpdateJobSeekerType = ClientInferResponseBody<
  typeof contract.user.updateJobseeker,
  200
>;

export enum yesOrNoEnum {
  yes = "Yes",
  no = "No",
}

export interface JobseekerSubscriptionInterface {
  pageSize: number;
  pageNumber: number;
}

export interface JobseekerDirectoryFilterInterface {
  searchText: string;
  languageIds: CustomSelectOption[] | null;
  minCTC: number | null;
  maxCTC: number | null;
  skills: CustomSelectOption[] | null;
  pageSize: number;
  pageNumber: number;
  activeSubscription: CustomSelectOption | null;
  noticePeriod: CustomSelectOption[] | null;
  locationIds: CustomSelectOption[] | null;
  experience: CustomSelectOption | null;
  ctc: CustomSelectOption | null;
  portfolio: CustomSelectOption | null;
  lastLogin: CustomSelectOption | null;
  preferredLocations: CustomSelectOption[] | null;
  customDate: [DateValue, DateValue] | null;
}

export interface ExperienceInYear {
  minExp: number;
  maxExp: number;
}
export const experienceInYearRanges = [
  { label: "Fresher", value: "0-0" }, // here value is in the format of min-max
  { label: "1-2 Years", value: "1-2" },
  { label: "3-5 Years", value: "3-5" },
  { label: "6-10 Years", value: "6-10" },
  { label: "10-15 Years", value: "10-15" },
  { label: "More than 15 Years", value: "15" },
];

export interface ExperienceInYear {
  minExp: number;
  maxExp: number;
}
