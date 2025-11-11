import AsyncSearchSelectField from "@components/form/AsyncSearchSelectField";
import { getQueryClient } from "api";
import { FilterInterface } from "@/types/jobs";
import { UseFormReturn } from "react-hook-form";
import { Image } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import {
  employmentModeOption,
  employmentTypeOption,
  salaryRanges,
} from "@/data/jobseeker";
import { getLocationOptionsData } from "@/data/common";

import { experienceInYearRanges } from "@/types/jobSeeker";

const JobsOtherFilters = ({
  hForm,
}: {
  hForm: UseFormReturn<FilterInterface, any, undefined>;
}) => {
  const tablet = useMediaQuery("(max-width: 992px)");
  return (
    <>
      <AsyncSearchSelectField
        hForm={hForm}
        name="location"
        placeholder="Location"
        isMulti
        getOptions={async (val: string) => {
          const data = await getQueryClient().job.getSuggestionLocation.query({
            query: { searchText: val },
          });
          if (data.status === 200) {
            return getLocationOptionsData({
              data: data.body,
            });
          }
          return [];
        }}
        instanceId="location"
        customStyles={{
          control: () => ({
            fontSize: tablet ? 14 : 16,
            backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
            minWidth: 260,
            maxWidth: 400,
          }),
        }}
        dropDownIcon={
          <Image
            src="/images/downArrow.svg"
            w={10}
            h={8}
            alt="indicator-icon"
          />
        }
      />
      <AsyncSearchSelectField
        hForm={hForm}
        name="employmentType"
        placeholder="Work Model"
        isMulti
        getOptions={async (val: string) => {
          return employmentTypeOption.filter((option) =>
            option.label.toLowerCase().includes(val.toLowerCase())
          );
        }}
        instanceId="employmentType"
        customStyles={{
          control: () => ({
            fontSize: tablet ? 14 : 16,
            backgroundColor: "var(--mantine-color-secondarySkyBlue-4)",
            minWidth: 280,
            maxWidth: 400,
          }),
        }}
        dropDownIcon={
          <Image
            src="/images/downArrow.svg"
            w={10}
            h={8}
            alt="indicator-icon"
          />
        }
      />
      <AsyncSearchSelectField
        hForm={hForm}
        name="salary"
        placeholder="Salary"
        getOptions={async (val: string) => {
          return salaryRanges.filter((option) =>
            option.label.toLowerCase().includes(val.toLowerCase())
          );
        }}
        instanceId="Salary"
        customStyles={{
          control: () => ({
            fontSize: tablet ? 14 : 16,
            backgroundColor: "var(--mantine-color-secondaryGreen-1)",
            minWidth: 200,
            maxWidth: 400,
          }),
        }}
        dropDownIcon={
          <Image
            src="/images/downArrow.svg"
            w={10}
            h={8}
            alt="indicator-icon"
          />
        }
      />
      <AsyncSearchSelectField
        hForm={hForm}
        name="workSchedule"
        placeholder="Work Mode"
        isMulti
        getOptions={async (val: string) => {
          return employmentModeOption.filter((option) =>
            option.label.toLowerCase().includes(val.toLowerCase())
          );
        }}
        instanceId="workSchedule"
        customStyles={{
          control: () => ({
            fontSize: tablet ? 14 : 16,
            backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
            minWidth: 220,
            maxWidth: 400,
          }),
        }}
        dropDownIcon={
          <Image
            src="/images/downArrow.svg"
            w={10}
            h={8}
            alt="indicator-icon"
          />
        }
      />
     
      <AsyncSearchSelectField
        hForm={hForm}
        name="industry"
        placeholder="Industry"
        isMulti
        getOptions={async (val: string) => {
          const data = await getQueryClient().job.getSuggestionIndustry.query({
            query: { searchText: val },
          });
          if (data.status === 200) {
            return data.body.map((eachValue) => ({
              label: eachValue.name,
              value: eachValue.id.toString(),
            }));
          }
          return [];
        }}
        instanceId="industry"
        customStyles={{
          control: () => ({
            fontSize: tablet ? 14 : 16,
            backgroundColor: "var(--mantine-color-secondarySkyBlue-4)",
            minWidth: 280,
            maxWidth: 400,
          }),
        }}
        dropDownIcon={
          <Image
            src="/images/downArrow.svg"
            w={10}
            h={8}
            alt="indicator-icon"
          />
        }
      />
       
      <AsyncSearchSelectField
        hForm={hForm}
        name="experience"
        placeholder="Experience" 
        getOptions={async (val: string) => {
          return experienceInYearRanges.filter((option) =>
            option.label.toLowerCase().includes(val.toLowerCase())
          );
        }}
        instanceId="Experience"
        customStyles={{
          control: () => ({
            fontSize: tablet ? 14 : 16,
            backgroundColor: "var(--mantine-color-secondaryGreen-1)",
            minWidth: 240,
            maxWidth: 500,
          }),
        }}
        dropDownIcon={
          <Image
            src="/images/downArrow.svg"
            w={10}
            h={8}
            alt="indicator-icon"
          />
        }
      />
       <AsyncSearchSelectField
        hForm={hForm}
        name="subFunctions"
        placeholder="Subfunction"
        isMulti
        getOptions={async (val: string) => {
          const data = await getQueryClient().job.getSuggestionSubFunction.query({
            query: { searchText: val },
          });
          if (data.status === 200) {
            return data.body.map((eachValue) => ({
              label: eachValue.name,
              value: eachValue.id.toString(),
            }));
          }
          return [];
        }}
        instanceId="subFunctions"
        customStyles={{
          control: () => ({
            fontSize: tablet ? 14 : 16,
            backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
            minWidth: 280,
            maxWidth: 400,
          }),
        }}
        dropDownIcon={
          <Image
            src="/images/downArrow.svg"
            w={10}
            h={8}
            alt="indicator-icon"
          />
        }
      />
    </>
  );
};
export default JobsOtherFilters;
