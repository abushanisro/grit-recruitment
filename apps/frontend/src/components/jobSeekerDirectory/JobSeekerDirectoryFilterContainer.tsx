import AsyncSearchSelectField from "@components/form/AsyncSearchSelectField";
import { getQueryClient } from "api";
import { UseFormReturn, useForm } from "react-hook-form";
import { Box, Divider, Flex, Group, Image, Stack } from "@mantine/core";
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
import CustomButton from "@components/buttons/CustomButton";
import SecondaryButton from "@components/buttons/SecondaryButton";
import { useRouter } from "next/router";
import { useQueryState } from "@/hooks/queryState";
import LabelText from "@components/profile/LabelText";
import { useEffect } from "react";
import { getAppliedFilterCount } from "@/utils/jobseekerDirectory";

const JobSeekerDirectoryFilterPageComponent = () => {
  const tablet = useMediaQuery("(max-width: 992px)");
  const router = useRouter();
  const defaultValueObj: JobseekerDirectoryFilterInterface = {
    searchText: "",
    pageNumber: 1,
    pageSize: 20,
    locationIds: null,
    maxCTC: null,
    minCTC: null,
    skills: null,
    activeSubscription: null,
    noticePeriod: null,
    ctc: null,
    experience: null,
    portfolio: null,
    lastLogin: null,
    preferredLocations: null,
    customDate: null,
    languageIds: null,
  };

  const [selectedFilters, setSelectedFilters] =
    useQueryState<JobseekerDirectoryFilterInterface>(
      "jobseekerDirectoryPageFilter",
      defaultValueObj
    );

  const hForm = useForm<JobseekerDirectoryFilterInterface>({
    mode: "onChange",
    defaultValues: selectedFilters,
  });

  const { reset } = hForm;

  useEffect(() => {
    reset(selectedFilters);
  }, [router.isReady]);

  const handleSubmit = (data: JobseekerDirectoryFilterInterface) => {
    const queryData = {
      jobseekerDirectoryPageFilter: JSON.stringify(data),
    };

    router.push({
      pathname: "/jobseekerDirectory",
      query: queryData,
    });
  };

  const handleClearFilters = () => {
    reset(defaultValueObj);
    setSelectedFilters(defaultValueObj);
  };

  const watchFormValues = hForm.watch();

  const appliedFiltersCount = getAppliedFilterCount(
    watchFormValues,
    defaultValueObj
  );

  return (
    <>
      <Stack pb={110}>
        <Flex justify="flex-start" wrap="wrap" gap={tablet ? 30 : 60}>
          <Group>
            <LabelText mt={10} c={"primarySkyBlue.6"} label="Location" />
            <Divider w={2} orientation="vertical" bg="primarySkyBlue.6" />
            <Box miw={160} maw={180}>
              <AsyncSearchSelectField
                hForm={hForm}
                name="locationIds"
                placeholder="Location"
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
                customStyles={{
                  control: () => ({
                    fontSize: tablet ? 14 : 16,
                    backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
                    minWidth: 200,
                    maxWidth: 400,
                  }),
                  placeholder: () => ({
                    whiteSpace: "nowrap",
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
            </Box>
          </Group>

          <Group>
            <LabelText mt={10} c={"primarySkyBlue.6"} label="Skills" />
            <Divider w={2} orientation="vertical" bg="primarySkyBlue.6" />
            <Box miw={160} maw={180}>
              <AsyncSearchSelectField
                hForm={hForm}
                name="skills"
                placeholder="Skills"
                isMulti
                getOptions={async (val: string) => {
                  const data =
                    await getQueryClient().job.getSuggestionSkill.query({
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
                customStyles={{
                  control: () => ({
                    fontSize: tablet ? 14 : 16,
                    backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
                    minWidth: 200,
                    maxWidth: 400,
                  }),
                  placeholder: () => ({
                    whiteSpace: "nowrap",
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
            </Box>
          </Group>
        </Flex>

        <Flex justify="flex-start" wrap="wrap" gap={tablet ? 30 : 60}>
          <Group>
            <LabelText mt={10} c={"primarySkyBlue.6"} label="Experience" />
            <Divider w={2} orientation="vertical" bg="primarySkyBlue.6" />
            <Box miw={160} maw={180}>
              <AsyncSearchSelectField
                hForm={hForm}
                name="experience"
                placeholder="Experience"
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
                customStyles={{
                  control: () => ({
                    fontSize: tablet ? 14 : 16,
                    backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
                    minWidth: 200,
                    maxWidth: 400,
                  }),
                  placeholder: () => ({
                    whiteSpace: "nowrap",
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
                showLoading={false}
              />
            </Box>
          </Group>

          <Group>
            <LabelText mt={10} c={"primarySkyBlue.6"} label="CTC" />
            <Divider w={2} orientation="vertical" bg="primarySkyBlue.6" />
            <Box miw={160} maw={180}>
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
                isSearchable={false}
                customStyles={{
                  control: () => ({
                    fontSize: tablet ? 14 : 16,
                    backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
                    minWidth: 200,
                    maxWidth: 400,
                  }),
                  placeholder: () => ({
                    whiteSpace: "nowrap",
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
                showLoading={false}
              />
            </Box>
          </Group>
        </Flex>
        <Flex justify="flex-start" wrap="wrap" gap={tablet ? 30 : 60}>
          <Group>
            <LabelText mt={10} c={"primarySkyBlue.6"} label="Portfolio" />
            <Divider w={2} orientation="vertical" bg="primarySkyBlue.6" />
            <Box miw={160} maw={180}>
              <AsyncSearchSelectField
                hForm={hForm}
                name="portfolio"
                placeholder="Portfolio"
                isSearchable={false}
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
                customStyles={{
                  control: () => ({
                    fontSize: tablet ? 14 : 16,
                    backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
                    minWidth: 200,
                    maxWidth: 400,
                  }),
                  placeholder: () => ({
                    whiteSpace: "nowrap",
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
                showLoading={false}
              />
            </Box>
          </Group>

          <Group>
            <LabelText mt={10} c={"primarySkyBlue.6"} label="Last login" />
            <Divider w={2} orientation="vertical" bg="primarySkyBlue.6" />
            <Box miw={160} maw={180}>
              <AsyncSearchSelectField
                hForm={hForm}
                name="lastLogin"
                isSearchable={false}
                placeholder="Last login"
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
                customStyles={{
                  control: () => ({
                    fontSize: tablet ? 14 : 16,
                    backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
                    minWidth: 200,
                    maxWidth: 400,
                  }),
                  placeholder: () => ({
                    whiteSpace: "nowrap",
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
                showLoading={false}
              />
            </Box>
          </Group>
        </Flex>
        <Flex justify="flex-start" wrap="wrap" gap={tablet ? 30 : 60}>
          <Group>
            <LabelText
              mt={10}
              c={"primarySkyBlue.6"}
              label="Preferred location"
            />
            <Divider w={2} orientation="vertical" bg="primarySkyBlue.6" />
            <Box miw={160} maw={180}>
              <AsyncSearchSelectField
                hForm={hForm}
                name="preferredLocations"
                placeholder="Preferred location"
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
                customStyles={{
                  control: () => ({
                    fontSize: tablet ? 14 : 16,
                    backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
                    minWidth: 200,
                    maxWidth: 400,
                  }),
                  placeholder: () => ({
                    whiteSpace: "nowrap",
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
                showLoading={false}
              />
            </Box>
          </Group>
          <Group>
            <LabelText mt={10} c={"primarySkyBlue.6"} label="Notice Period" />
            <Divider w={2} orientation="vertical" bg="primarySkyBlue.6" />
            <Box miw={160} maw={180}>
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
                isSearchable={false}
                customStyles={{
                  control: () => ({
                    fontSize: tablet ? 14 : 16,
                    backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
                    minWidth: 200,
                    maxWidth: 400,
                  }),
                  placeholder: () => ({
                    whiteSpace: "nowrap",
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
                showLoading={false}
              />
            </Box>
          </Group>
        </Flex>

        <Flex justify="flex-start" wrap="wrap" gap={tablet ? 30 : 60}>
          <Group>
            <LabelText mt={10} c={"primarySkyBlue.6"} label="Language" />
            <Divider w={2} orientation="vertical" bg="primarySkyBlue.6" />
            <Box miw={160} maw={180}>
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
                instanceId="languageIds"
                customStyles={{
                  control: () => ({
                    fontSize: tablet ? 14 : 16,
                    backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
                    minWidth: 200,
                    maxWidth: 400,
                  }),
                  placeholder: () => ({
                    whiteSpace: "nowrap",
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
                showLoading={false}
              />
            </Box>
          </Group>
        </Flex>
        <FilterButtons
          appliedFiltersCount={appliedFiltersCount}
          hForm={hForm}
          handleSubmit={() => handleSubmit(hForm.getValues())}
          handleClearFilters={handleClearFilters}
        />
      </Stack>
    </>
  );
};

const FilterButtons = ({
  appliedFiltersCount,
  hForm,
  handleSubmit,
  handleClearFilters,
}: {
  appliedFiltersCount: number;
  hForm: UseFormReturn<JobseekerDirectoryFilterInterface>;
  handleSubmit: () => void;
  handleClearFilters: () => void;
}) => {
  const isMobile = useMediaQuery("(max-width: 768px)");

  const content = (
    <>
      <Group>
        <CustomButton
          onClick={hForm.handleSubmit(handleSubmit)}
          label={`Apply ${
            appliedFiltersCount ? "(" + appliedFiltersCount + ")" : ""
          } filters & show results`}
          fw="700"
        />
        <SecondaryButton
          onClick={handleClearFilters}
          label="Clear filters"
          fw="700"
        />
      </Group>
    </>
  );

  return isMobile ? (
    <Box
      style={{ position: "fixed", bottom: "10vh", width: "100%", zIndex: 50 }}
    >
      {content}
    </Box>
  ) : (
    <Box pt={15}>{content}</Box>
  );
};

export default JobSeekerDirectoryFilterPageComponent;
