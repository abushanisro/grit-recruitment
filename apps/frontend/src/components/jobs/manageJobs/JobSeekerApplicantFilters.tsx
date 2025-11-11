import AsyncSearchSelectField from "@components/form/AsyncSearchSelectField";
import { getQueryClient } from "api";
import { UseFormReturn } from "react-hook-form";
import { Box, Image, Text, Stack, Group } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import {  NoticePeriodEnum } from "contract/enum";
import JobSearchField from "@components/jobs/filters/JobSearchField";
import isEmpty from "lodash/isEmpty";
import { getNoticePeriodLabel } from "@/utils/common";
import { JobSeekerFilterRequest } from "@/types/employer";
import { salaryRanges } from "@/data/jobseeker";
import getFilteredLocationOptionsData from "@/data/common";
import { experienceInYearRanges } from "@/types/jobSeeker";

const JobSeekerApplicantFilters = ({
  title,
  hForm,
}: {
  title?: string;
  hForm: UseFormReturn<JobSeekerFilterRequest>;
}) => {
  const tablet = useMediaQuery("(max-width: 992px)");
  return (
    <Stack gap={0}>
      {title && (
        <Text fz={20} fw={500} c="white" mt={12}>
          {title}
        </Text>
      )}
      <Box w="100%" maw="450px">
        <JobSearchField
          name="jobSeekerName"
          hForm={hForm}
          onReset={() => hForm.setValue("jobSeekerName", "")}
          placeholder="Search Candidates"
          fz={{ base: 20, sm: 24 }}
          fw={700}
        />
      </Box>

      <Group
        mt={20}
        maw={{ base: "100%", md: "100%", lg: "80%" }}
        style={{
          alignItems: "flex-start",
        }}
      >
        <AsyncSearchSelectField
          hForm={hForm}
          name="noticePeriod"
          placeholder="Notice Period"
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
          customStyles={{
            control: () => ({
              fontSize: tablet ? 14 : 16,
              backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
              minWidth: 180,
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
          name="locationIds"
          placeholder="Location"
          isMulti
          getOptions={async (val: string) => {
            const data = await getQueryClient().job.getSuggestionLocation.query(
              {
                query: { searchText: val },
              }
            );
            if (data.status === 200) {
              return getFilteredLocationOptionsData({
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
              minWidth: 130,
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
              backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
              minWidth: 160,
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
          name="ctc"
          placeholder="CTC"
          getOptions={async (val: string) => {
            return salaryRanges.filter((option) =>
              option.label.toLowerCase().includes(val.toLowerCase())
            );
          }}
          instanceId="CTC"
          customStyles={{
            control: () => ({
              fontSize: tablet ? 14 : 16,
              backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
              minWidth: 200,
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
          name="languageIds"
          placeholder="Language"
          isMulti
          getOptions={async (val: string) => {
            const data =
              await getQueryClient().job.getSuggestionLanguages.query({
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
          instanceId="language"
          customStyles={{
            control: () => ({
              fontSize: tablet ? 14 : 16,
              backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
              minWidth: 200,
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
          name="skillIds"
          placeholder="Skills"
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
          instanceId="skill"
          customStyles={{
            control: () => ({
              fontSize: tablet ? 14 : 16,
              backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
              minWidth: 200,
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
      </Group>
    </Stack>
  );
};

export default JobSeekerApplicantFilters;
