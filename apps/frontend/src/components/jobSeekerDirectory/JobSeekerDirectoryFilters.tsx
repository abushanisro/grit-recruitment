import AsyncSearchSelectField from "@components/form/AsyncSearchSelectField";
import { getQueryClient } from "api";
import { UseFormReturn } from "react-hook-form";
import { useMediaQuery } from "@mantine/hooks";
import isEmpty from "lodash/isEmpty";
import {
  DateFilterEnum,
  ExperienceInYear,
  NoticePeriodEnum,
} from "contract/enum";

import {
  getNoticePeriodLabel,
  getExperienceLabel,
  getDateFromEnum,
  getDateLabelEnum,
} from "@/utils/common";

import { salaryRanges } from "@/data/jobseeker";
import getLocationOptionsDataBasedOnUserRole from "@/data/common";
import {
  JobseekerDirectoryFilterInterface,
  yesOrNoEnum,
} from "@/types/jobSeeker";
import { Flex } from "@mantine/core";

const SelectFieldWrapper = ({ children }: { children: React.ReactNode }) => {
  return (
    <Flex wrap="nowrap" gap={10}>
      {children}
    </Flex>
  );
};

const JobSeekeerDirectoryFilters = ({
  hForm,
}: {
  hForm: UseFormReturn<JobseekerDirectoryFilterInterface>;
}) => {
  const tablet = useMediaQuery("(max-width: 992px)");

  const { getValues } = hForm;

  const formValues = getValues();

  const locationIds = formValues.locationIds;
  const skills = formValues.skills;
  const preferredLocations = formValues.preferredLocations;
  const noticePeriod = formValues.noticePeriod;
  const experience = formValues.experience;
  const ctc = formValues.ctc;
  const portfolio = formValues.portfolio;
  const lastLogin = formValues.lastLogin;
  const languageIds = formValues.languageIds;

  const customStyles = {
    control: () => ({
      fontSize: tablet ? 14 : 16,
      backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
      minWidth: 220,
      maxWidth: 400,
    }),
    multiValueRemove: () => ({
      display: "none",
    }),
  };

  const labelProps = {
    style: {
      fontSize: 16,
      color: "var(--mantine-color-primarySkyBlue-6)",
      fontWeight: "bold" as const,
    },
    my: "auto",
  };

  return (
    <>
      <SelectFieldWrapper>
        {!isEmpty(locationIds) && (
          <AsyncSearchSelectField
            hForm={hForm}
            name="locationIds"
            placeholder="Location"
            label="Location:"
            isMulti
            getOptions={async (val: string) => {
              const data =
                await getQueryClient().job.getSuggestionLocation.query({
                  query: { searchText: val },
                });
              if (data.status === 200) {
                return getLocationOptionsDataBasedOnUserRole({
                  data: data.body,
                });
              }
              return [];
            }}
            instanceId="location"
            customStyles={customStyles}
            labelProps={labelProps}
            isSearchable={false}
            isClearable={false}
            isDisabled={true}
            showLoading={false}
          />
        )}
      </SelectFieldWrapper>
      <SelectFieldWrapper>
        {!isEmpty(skills) && (
          <AsyncSearchSelectField
            hForm={hForm}
            name="skills"
            placeholder="Skills"
            label="Skills:"
            isMulti
            getOptions={async (val: string) => {
              const data = await getQueryClient().job.getSuggestionSkill.query({
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
            instanceId="skills"
            customStyles={customStyles}
            labelProps={labelProps}
            isSearchable={false}
            isClearable={false}
            isDisabled={true}
            showLoading={false}
          />
        )}
      </SelectFieldWrapper>
      <SelectFieldWrapper>
        {!isEmpty(preferredLocations) && (
          <AsyncSearchSelectField
            hForm={hForm}
            name="preferredLocations"
            placeholder="Preferred location"
            label="Preferred location:"
            isMulti
            getOptions={async (val: string) => {
              const data =
                await getQueryClient().job.getSuggestionLocation.query({
                  query: { searchText: val },
                });
              if (data.status === 200) {
                return getLocationOptionsDataBasedOnUserRole({
                  data: data.body,
                });
              }
              return [];
            }}
            instanceId="preferredlocations"
            customStyles={customStyles}
            labelProps={labelProps}
            isSearchable={false}
            isClearable={false}
            isDisabled={true}
            showLoading={false}
          />
        )}
      </SelectFieldWrapper>
      <SelectFieldWrapper>
        {!isEmpty(noticePeriod) && (
          <AsyncSearchSelectField
            hForm={hForm}
            name="noticePeriod"
            placeholder="Notice Period"
            label="Notice Period:"
            isMulti
            getOptions={async (val: string) => {
              return Object.values(NoticePeriodEnum)
                .filter(
                  (period) =>
                    isEmpty(val) ||
                    val.toLowerCase().includes(period.toLowerCase())
                )
                .map((period) => ({
                  label: getNoticePeriodLabel(period),
                  value: period,
                }));
            }}
            instanceId="noticePeriod"
            customStyles={customStyles}
            labelProps={labelProps}
            isSearchable={false}
            isClearable={false}
            isDisabled={true}
            showLoading={false}
          />
        )}
      </SelectFieldWrapper>
      <SelectFieldWrapper>
        {!isEmpty(experience) && (
          <AsyncSearchSelectField
            hForm={hForm}
            name="experience"
            placeholder="Experience"
            label="Experience:"
            getOptions={async (val: string) => {
              return Object.values(ExperienceInYear)
                .map((exp) => ({
                  label: getExperienceLabel(exp),
                  value: exp.toString(),
                }))
                .filter(
                  (exp) =>
                    isEmpty(val) ||
                    exp.label.toLowerCase().includes(val.toLowerCase())
                );
            }}
            instanceId="Experience"
            customStyles={customStyles}
            labelProps={labelProps}
            isSearchable={false}
            isClearable={false}
            isDisabled={true}
            showLoading={false}
          />
        )}
      </SelectFieldWrapper>
      <SelectFieldWrapper>
        {!isEmpty(ctc) && (
          <AsyncSearchSelectField
            hForm={hForm}
            name="ctc"
            placeholder="CTC"
            label="CTC:"
            getOptions={async (val: string) => {
              return salaryRanges.filter((option) =>
                option.label.toLowerCase().includes(val.toLowerCase())
              );
            }}
            instanceId="CTC"
            customStyles={customStyles}
            labelProps={labelProps}
            isSearchable={false}
            isClearable={false}
            isDisabled={true}
            showLoading={false}
          />
        )}
      </SelectFieldWrapper>
      <SelectFieldWrapper>
        {!isEmpty(portfolio) && (
          <AsyncSearchSelectField
            hForm={hForm}
            name="portfolio"
            placeholder="Portfolio"
            label="Portfolio:"
            getOptions={async (val: string) => {
              return Object.values(yesOrNoEnum)
                .filter(
                  (option) =>
                    isEmpty(val) ||
                    val.toLowerCase().includes(option.toLowerCase())
                )
                .map((option) => ({
                  label: option,
                  value: option,
                }));
            }}
            instanceId="Porfolio"
            customStyles={customStyles}
            labelProps={labelProps}
            isSearchable={false}
            isClearable={false}
            isDisabled={true}
            showLoading={false}
          />
        )}
      </SelectFieldWrapper>
      <SelectFieldWrapper>
        {!isEmpty(lastLogin) && (
          <AsyncSearchSelectField
            hForm={hForm}
            name="lastLogin"
            placeholder="Last login"
            label="Last login:"
            getOptions={async (val: string) => {
              return Object.values(DateFilterEnum)
                .filter(
                  (date) =>
                    isEmpty(val) ||
                    val.toLowerCase().includes(date.toLowerCase())
                )
                .map((date) => ({
                  label: getDateLabelEnum(date),
                  value: getDateFromEnum(date),
                }));
            }}
            instanceId="lastLogin"
            customStyles={customStyles}
            labelProps={labelProps}
            isSearchable={false}
            isClearable={false}
            isDisabled={true}
            showLoading={false}
          />
        )}
      </SelectFieldWrapper>
      <SelectFieldWrapper>
        {!isEmpty(languageIds) && (
          <AsyncSearchSelectField
            hForm={hForm}
            name="languageIds"
            placeholder="Language"
            label="Language:"
            isMulti
            getOptions={async (val: string) => {
              const data =
                await getQueryClient().job.getSuggestionLanguages.query({
                  query: { searchText: val },
                });
              if (data.status === 200) {
                return getLocationOptionsDataBasedOnUserRole({
                  data: data.body,
                });
              }
              return [];
            }}
            instanceId="languageIds"
            customStyles={customStyles}
            labelProps={labelProps}
            isSearchable={false}
            isClearable={false}
            isDisabled={true}
            showLoading={false}
          />
        )}
      </SelectFieldWrapper>
    </>
  );
};
export default JobSeekeerDirectoryFilters;
