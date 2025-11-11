import { EmploymentMode, EmploymentType } from "contract/enum";

export const salaryRanges = [
  { label: "0 - 4 LPA", value: "0-4" },
  { label: "4 - 6 LPA", value: "4-6" },
  { label: "6 - 10 LPA", value: "6-10" },
  { label: "10 - 18 LPA", value: "10-18" },
  { label: "18+ LPA", value: "18" },
];

export const experienceInYearRanges = [
  { label: "Fresher", value: "0-0" }, // here value is in the format of min-max
  { label: "1-2 Years", value: "1-2" },
  { label: "3-5 Years", value: "3-5" },
  { label: "6-10 Years", value: "6-10" },
  { label: "10-15 Years", value: "10-15" },
  { label: "More than 15 Years", value: "15" },
];

export const portfolioOption = [
  { label: "Yes", value: true },
  { label: "No", value: false },
];

export const employmentTypeOption = [
  { label: "Full Time", value: EmploymentType.FullTime },
  { label: "Part Time", value: EmploymentType.PartTime },
  { label: "Internship", value: EmploymentType.Internship },
];
export const employmentModeOption = [
  { label: "Remote", value: EmploymentMode.Remote },
  { label: "Hybrid", value: EmploymentMode.Hybrid },
  { label: "Onsite", value: EmploymentMode.Onsite },
];
