import { ModifiedCreateJobType } from "@/types/jobs";
import { useForm } from "react-hook-form";
import { getQueryClient } from "api";
import { EmployerUserDataType } from "@/types/employer";
import { useApi } from "@/hooks/useApi";
import { useRouter } from "next/router";
import SectionContainer from "@components/SectionContainer";
import CustomInputField from "@components/form/CustomInputField";
import AsyncSearchSelectField from "@components/form/AsyncSearchSelectField";
import { useDisclosure, useMediaQuery } from "@mantine/hooks";
import {
  Box,
  Button,
  Divider,
  Flex,
  Grid,
  Group,
  Image,
  Stack,
  Text,
} from "@mantine/core";
import LabelText from "@components/profile/LabelText";
import CustomNumberInputField from "@components/form/CustomNumberInput";
import { getSelectFieldStyles } from "@/utils/profile";
import PrimaryButton from "@components/buttons/PrimaryButton";
import SecondaryButton from "@components/buttons/SecondaryButton";
import isNil from "lodash/isNil";
import { EmploymentMode, EmploymentType } from "contract/enum";
import { ActiveTabButton } from "@components/buttons/ActiveTabButton";
import isEmpty from "lodash/isEmpty";
import { employmentModeOption, employmentTypeOption } from "@/data/jobseeker";
import { getLocationOptionsData } from "@/data/common";
import classes from "@/components/form/Form.module.css";
import { useEditor } from "@tiptap/react";
import { richTextEditorExtension } from "@/data/commonExtensions";
import RichTextEditorForm from "@components/RichTextEditorForm";
import { useUserData } from "@/contexts/UserProvider";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import { PostPaidActiveSubscriptionResponseType } from "@/types/postPaid";
import { contract } from "contract";
import { useEffect, useState } from "react";
import ErrorMessage from "@components/ErrorMessage";
import { get } from "lodash";
import JobFormSubscribeModal from "@components/JobFormSubscribeModal";
import PostPaidJobSeeekerFormModal from "@components/PostPaidJobSeekerFormModal";

const createJobDefaultValues: ModifiedCreateJobType = {
  title: null,
  companyId: null,
  companyName: "",
  cityIds: [],
  languageIds: [],
  industryIds: null,
  subfunctionIds: null,
  description: "",
  employmentTypes: null,
  employmentModes: null,
  minExperienceInYears: null,
  maxExperienceInYears: null,
  isPosted: false,
  isPremium: false,
  subscriptionId: null,
  isBoosted: false,
};

const JobSeekerJobForm = ({ data }: { data: EmployerUserDataType }) => {
  const { makeApiCall } = useApi();
  const tablet = useMediaQuery("(max-width: 992px)");
  const content = "";

  const mobile = useMediaQuery("(max-width: 525px)");
  const router = useRouter();
  const { userDetails } = useUserData();

  const editor = useEditor({
    extensions: richTextEditorExtension,
    content,
    editable: true,
  });
  const [subscriptionName, setSubscriptionName] = useState<
    string | null | undefined
  >();
  const [boostJobOpened, { open: boostJobOpen, close: boostJobClose }] =
    useDisclosure(false);
  const [
    unPromotedJobOpened,
    { open: unPromoteJobOpen, close: unPromoteJobClose },
  ] = useDisclosure(false);
  const hForm = useForm<ModifiedCreateJobType>({
    mode: "onChange",
    defaultValues: {
      ...createJobDefaultValues,
      companyName: data.company.name,
      companyId: data.company.companyId,
    },
  });

  const {
    watch,
    setValue,
    clearErrors,
    formState: { errors },
    trigger,
    register,
    getValues,
  } = hForm;

  const isBoosted = watch("isBoosted");
  const subscriptionId = watch("subscriptionId");
  useEffect(() => {
    register("isBoosted", {
      validate: (value) => {
        if (!value) {
          clearErrors("isBoosted");
          return true;
        }
        if (value && userDetails && !userDetails.isJobBoostPurchaseActive) {
          return "Please buy job boost subscription";
        }

        if (
          value &&
          userDetails &&
          userDetails.isJobBoostPurchaseActive &&
          isNil(getValues("subscriptionId"))
        ) {
          return "Please select a subscription to boost the job";
        }
        clearErrors("isBoosted");
        return true;
      },
    });
  }, [userDetails?.isJobBoostPurchaseActive]);

  useEffect(() => {
    trigger("isBoosted");
  }, [subscriptionId, userDetails?.isJobBoostPurchaseActive]);

  const handleBuyPremiumPlan = () => {
    setValue("isBoosted", true);
    trigger("isBoosted");
    if (
      (userDetails && !userDetails.isJobBoostPurchaseActive) ||
      activeSubscriptionData.length === 0
    ) {
      unPromoteJobOpen();
      return;
    }
    boostJobOpen();
  };

  const queryObj = {
    pageSize: String(5),
    pageNumber: String(1),
  };
  const {
    data: subscriptionPlanData,
    isLoading,
    error,
  } = getQueryClient().paidJobs.getActiveSubscriptions.useQuery(
    [contract.paidJobs.getActiveSubscriptions.path, queryObj],
    {
      query: queryObj,
    }
  );
  const activeSubscriptionData =
    subscriptionPlanData?.body.results.filter(
      (eachSubscription: PostPaidActiveSubscriptionResponseType) =>
        eachSubscription.boostLimit > 0
    ) ?? [];

  if (isLoading) {
    return (
      <Grid
        maw={{ base: 400, sm: 500, md: 700, lg: 1100 }}
        mx="auto"
        my={{ base: 40, md: 58 }}
      >
        <Grid.Col span={{ base: 12, lg: 8 }}>
          <JobListSkeletonCard />
        </Grid.Col>
      </Grid>
    );
  }
  if (error) {
    return <ErrorMessage error={error} />;
  }
  const minExperience = watch("minExperienceInYears");
  const maxExperience = watch("maxExperienceInYears");
  const minSalaryInLpa = watch("minSalaryInLpa");
  const maxSalaryInLpa = watch("maxSalaryInLpa");
  const boostError = get(errors, "isBoosted");
  const handleSubmit = (payload: ModifiedCreateJobType) => {
    makeApiCall({
      fetcherFn: async () => {
        const response =
          await getQueryClient().job.createJobForJobSeeker.mutation({
            body: {
              ...payload,
              companyId: !isNil(payload.companyId) ? payload.companyId : 0,
              title: payload.title ?? "",
              employmentTypes:
                payload.employmentTypes?.map((eachValue) => {
                  return eachValue.value as EmploymentType;
                }) ?? [],
              description: JSON.stringify(editor?.getJSON()),
              employmentModes:
                payload.employmentModes?.map((eachValue) => {
                  return eachValue.value as EmploymentMode;
                }) ?? [],
              industryIds:
                payload.industryIds?.map((eachValue) => {
                  return Number(eachValue.value);
                }) ?? [],
              subfunctionIds:
                payload.subfunctionIds?.map((eachValue) => {
                  return Number(eachValue.value);
                }) ?? [],
              cityIds:
                payload.cityIds?.map((eachValue) => {
                  return Number(eachValue.value);
                }) ?? [],
              maxExperienceInYears: !isNil(payload.maxExperienceInYears)
                ? Number(payload.maxExperienceInYears)
                : 0,
              minExperienceInYears: !isNil(payload.minExperienceInYears)
                ? Number(payload.minExperienceInYears)
                : 0,
              maxSalaryInLpa: !isNil(payload.maxSalaryInLpa)
                ? Number(payload.maxSalaryInLpa)
                : undefined, //TODO : need to change in backend to send null instead
              minSalaryInLpa: !isNil(payload.minSalaryInLpa)
                ? Number(payload.minSalaryInLpa)
                : undefined,
              isPremium: payload.isBoosted,
              subscriptionId: payload.subscriptionId
                ? String(payload.subscriptionId)
                : undefined,
            },
          });
        return response;
      },
      successMsgProps: { message: "Job created successfully" },
      onSuccessFn: () => {
        router.push('manageJobs/?activeTab="jobSeeker"');
      },
      showFailureMsg: true,
    });
  };

  return (
    <Box mt={{ base: 32, sm: 48 }}>
      <JobFormSubscribeModal
        opened={unPromotedJobOpened}
        onClose={unPromoteJobClose}
      />
      <PostPaidJobSeeekerFormModal
        hForm={hForm}
        activeSubscriptionData={activeSubscriptionData}
        setSubscriptionName={setSubscriptionName}
        opened={boostJobOpened}
        onClose={boostJobClose}
      />
      <Box pl={{ base: 0, sm: 30, md: 50 }}>
        <ActiveTabButton isActive={true} label="POST A JOB - JOBSEEKER" />
      </Box>
      <SectionContainer
        p={{ base: 20, sm: 30, md: 60 }}
        mb={40}
        style={{ overflow: "visible" }}
        h="max-content"
      >
        <form onSubmit={hForm.handleSubmit(handleSubmit)}>
          <Stack gap={24}>
            <Stack gap={0}>
              <LabelText
                label="Job Title"
                rules={{ required: "Job title is required" }}
              />
              <CustomInputField
                hForm={hForm}
                name="title"
                placeholder="Enter a job title"
                miw={{ base: "100%", md: 400 }}
                rules={{
                  required: "Job title is required",
                  validate: {
                    checkNameLength: (value) => {
                      if (value.length < 1 || value.length > 150) {
                        return "Job title must be between 1 and 150 characters.";
                      }
                      return true; // Validation passed
                    },
                  },
                }}
              />
            </Stack>
            <CustomInputField
              hForm={hForm}
              name="companyName"
              label="Company Name"
              disabled={true}
              rules={{
                required: "Company Name is required",
                validate: {
                  checkNameLength: (value) => {
                    if (value.length < 2 || value.length > 500) {
                      return "Company Name must be between 2 and 500 characters.";
                    }
                    return true; // Validation passed
                  },
                },
              }}
              minLength={2}
              maxLength={500}
            />
            <Box
              bg="#1f3243"
              p={20}
              style={{ borderRadius: 15 }}
              maw={{ base: "100%", sm: 400, md: 600 }}
            >
              <LabelText label="Salary" />
              <Flex justify="flex-start" wrap="wrap" gap={tablet ? 30 : 60}>
                <Group>
                  <LabelText label="Min" pb={0} />
                  <Divider w={2} orientation="vertical" bg="secondaryGreen.1" />
                  {/* TODO: make a common component for CustomNumberInputField in this form */}
                  <CustomNumberInputField
                    hForm={hForm}
                    allowNegative={false}
                    className={classes.numberInput}
                    decimalScale={2}
                    name="minSalaryInLpa"
                    placeholder="E.g 1"
                    maw={130}
                    miw={100}
                    rules={{
                      required: false,
                      validate: {
                        checkBoundary: (value) => {
                          const numValue = parseFloat(value);
                          if (numValue === 0) {
                            return "Min salary cannot be 0";
                          }
                          if (numValue < 0 || numValue > 50) {
                            return "Min salary must be between 0 and 50.";
                          }
                          if (
                            !isNil(maxSalaryInLpa) &&
                            !isEmpty(maxSalaryInLpa) &&
                            numValue >= maxSalaryInLpa
                          ) {
                            return "Min salary cannot be greater than or equal to max salary";
                          }
                          return true; // Validation passed
                        },
                      },
                    }}
                    rightSection={
                      <Text mt={-5} fz={{ base: 17 }} c="#dfdfdf" fw={700}>
                        LPA
                      </Text>
                    }
                    inputStyles={{
                      styles: {
                        section: { margin: 20, zIndex: 0 },
                        input: {
                          color: "white",
                          background: "transparent",
                          borderRadius: 30,
                          borderColor: "white",
                          fontSize: tablet ? 16 : 20,
                          paddingInline: 20,
                          paddingBlock: 0,
                        },
                      },
                    }}
                  />
                </Group>

                <Group>
                  <LabelText label="Max" pb={0} />
                  <Divider w={2} orientation="vertical" bg="secondaryGreen.1" />
                  <CustomNumberInputField
                    hForm={hForm}
                    allowNegative={false}
                    decimalScale={2}
                    className={classes.numberInput}
                    name="maxSalaryInLpa"
                    placeholder="E.g 5"
                    maw={130}
                    miw={100}
                    rules={{
                      required: false,
                      validate: {
                        checkBoundary: (value) => {
                          const numValue = parseFloat(value);
                          if (numValue === 0) {
                            return "Max salary cannot be 0";
                          }
                          if (numValue < 0 || numValue > 50) {
                            return "Max salary must be between 0 and 50.";
                          }
                          if (
                            !isNil(minSalaryInLpa) &&
                            !isEmpty(minSalaryInLpa) &&
                            minSalaryInLpa >= numValue
                          ) {
                            return "Max salary cannot be lesser than or equal to min salary";
                          }
                          return true; // Validation passed
                        },
                      },
                    }}
                    rightSection={
                      <Text mt={-5} fz={{ base: 17 }} c="#dfdfdf" fw={700}>
                        LPA
                      </Text>
                    }
                    inputStyles={{
                      styles: {
                        section: { margin: 20, zIndex: 0 },
                        input: {
                          color: "white",
                          background: "transparent",
                          borderRadius: 30,
                          borderColor: "white",
                          fontSize: tablet ? 16 : 20,
                          paddingInline: 20,
                          paddingBlock: 0,
                        },
                      },
                    }}
                  />
                </Group>
              </Flex>
            </Box>
            <Box
              bg="#1f3243"
              p={20}
              style={{ borderRadius: 15 }}
              maw={{ base: "100%", sm: 400, md: 600 }}
            >
              <LabelText label="Experience" />
              <Flex justify="flex-start" wrap="wrap" gap={tablet ? 30 : 50}>
                <Group>
                  <LabelText
                    label="Min"
                    rules={{ required: "Min. Experience is required" }}
                    pb={0}
                  />
                  <Divider w={2} orientation="vertical" bg="secondaryGreen.1" />
                  <CustomNumberInputField
                    hForm={hForm}
                    className={classes.numberInput}
                    allowNegative={false}
                    decimalScale={2}
                    name="minExperienceInYears"
                    placeholder="E.g 1"
                    maw={130}
                    miw={100}
                    min={0}
                    max={25}
                    rules={{
                      required: "Min. Experience is required",
                      validate: {
                        checkBoundary: (value) => {
                          const numValue = parseFloat(value);

                          if (numValue < 0 || numValue > 25) {
                            return "Min Experiencere must be between 0 and 25.";
                          }
                          if (numValue == 0 && maxExperience == 0) {
                            return true;
                          }

                          if (
                            !isNil(maxExperience) &&
                            !isEmpty(maxExperience) &&
                            numValue >= maxExperience
                          ) {
                            return "Min Experience cannot be greater than or equal to max experience";
                          }
                          return true; // Validation passed
                        },
                      },
                    }}
                    rightSection={
                      <Text mt={-5} fz={{ base: 17 }} fw={700}>
                        years
                      </Text>
                    }
                    inputStyles={{
                      styles: {
                        section: { margin: 20, zIndex: 0 },
                        input: {
                          color: "white",
                          background: "transparent",
                          borderRadius: 30,
                          fontSize: tablet ? 16 : 20,
                          paddingInline: 20,
                          paddingBlock: 0,
                        },
                      },
                    }}
                  />
                </Group>

                <Group>
                  <LabelText
                    label="Max"
                    rules={{ required: "Max. Experience is required" }}
                    pb={0}
                  />
                  <Divider w={2} orientation="vertical" bg="secondaryGreen.1" />
                  <CustomNumberInputField
                    hForm={hForm}
                    className={classes.numberInput}
                    allowNegative={false}
                    decimalScale={2}
                    name="maxExperienceInYears"
                    placeholder="E.g 1"
                    maw={130}
                    miw={100}
                    min={0}
                    max={25}
                    rules={{
                      required: "Max. Experience is required",
                      validate: {
                        checkBoundary: (value) => {
                          const numValue = parseFloat(value);

                          if (numValue < 0 || numValue > 25) {
                            return "Max Experience must be between 0 and 25.";
                          }
                          if (numValue == 0 && minExperience == 0) {
                            return true;
                          }
                          if (
                            !isNil(minExperience) &&
                            !isEmpty(minExperience) &&
                            minExperience >= numValue
                          ) {
                            return "Max Experience cannot be lesser than or equal to min experience";
                          }
                          return true; // Validation passed
                        },
                      },
                    }}
                    rightSection={
                      <Text mt={-5} fz={{ base: 17 }} fw={700}>
                        years
                      </Text>
                    }
                    inputStyles={{
                      styles: {
                        section: { margin: 20, zIndex: 0 },
                        input: {
                          color: "white",
                          background: "transparent",
                          borderRadius: 30,
                          fontSize: tablet ? 16 : 20,
                          paddingInline: 20,
                          paddingBlock: 0,
                        },
                      },
                    }}
                  />
                </Group>
              </Flex>
            </Box>
            <Flex justify="flex-start" wrap="wrap" gap={tablet ? 30 : 60}>
              <Group>
                <LabelText
                  label="Location"
                  rules={{ required: "Location is required" }}
                  pb={0}
                />
                <Divider w={2} orientation="vertical" bg="secondaryGreen.1" />
                <Box w={200}>
                  <AsyncSearchSelectField
                    hForm={hForm}
                    name="cityIds"
                    placeholder="Type here"
                    rules={{ required: "Location is required" }}
                    isMulti
                    getOptions={async (val: string) => {
                      const data =
                        await getQueryClient().job.getSuggestionLocation.query({
                          query: { searchText: val },
                        });
                      if (data.status === 200) {
                        return getLocationOptionsData({
                          data: data.body,
                        });
                      }
                      return [];
                    }}
                    instanceId="cityIds"
                    customStyles={getSelectFieldStyles(tablet)}
                    dropDownIcon={
                      <Image
                        src="/images/greenDownArrow.svg"
                        w={10}
                        h={8}
                        alt="indicator-icon"
                      />
                    }
                  />
                </Box>
              </Group>
            </Flex>

            <Box>
              <Flex justify="flex-start" wrap="wrap" gap={tablet ? 10 : 30}>
                <Box>
                  <AsyncSearchSelectField
                    hForm={hForm}
                    name="employmentTypes"
                    placeholder="Type of Employment"
                    rules={{ required: "Type of Employment is required" }}
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
                        backgroundColor: "var(--mantine-color-primaryGreen-3)",
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
                </Box>{" "}
                <Box>
                  <AsyncSearchSelectField
                    hForm={hForm}
                    name="employmentModes"
                    placeholder="Work Schedule"
                    rules={{ required: "Work Schedule is required" }}
                    isMulti
                    getOptions={async (val: string) => {
                      return employmentModeOption.filter((option) =>
                        option.label.toLowerCase().includes(val.toLowerCase())
                      );
                    }}
                    instanceId="employmentModes"
                    customStyles={{
                      control: () => ({
                        fontSize: tablet ? 14 : 16,
                        backgroundColor: "var(--mantine-color-primaryGreen-3)",
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
              </Flex>
              <Flex
                justify="flex-start"
                wrap="wrap"
                gap={tablet ? 10 : 30}
                mt={20}
              >
                <Box>
                  <AsyncSearchSelectField
                    hForm={hForm}
                    name="industryIds"
                    placeholder="Industry"
                    rules={{ required: "Industry is required" }}
                    isMulti
                    getOptions={async (val: string) => {
                      const data =
                        await getQueryClient().job.getSuggestionIndustry.query({
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
                    instanceId="industryIds"
                    customStyles={{
                      control: () => ({
                        fontSize: tablet ? 14 : 16,
                        maxWidth: mobile ? 300 : 500,
                        minWidth: 200,
                        backgroundColor:
                          "var(--mantine-color-primarySkyBlue-6)",
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
                <Box>
                  <AsyncSearchSelectField
                    hForm={hForm}
                    name="subfunctionIds"
                    placeholder="Sub Function"
                    rules={{ required: "Sub Function is required" }}
                    isMulti
                    getOptions={async (val: string) => {
                      const data =
                        await getQueryClient().job.getSuggestionSubFunction.query(
                          {
                            query: { searchText: val },
                          }
                        );
                      if (data.status === 200) {
                        return data.body.map((eachValue) => ({
                          label: eachValue.name,
                          value: eachValue.id.toString(),
                        }));
                      }
                      return [];
                    }}
                    instanceId="subfunctionIds"
                    customStyles={{
                      control: () => ({
                        fontSize: tablet ? 14 : 16,
                        backgroundColor:
                          "var(--mantine-color-secondaryGreen-1)",
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
              </Flex>
            </Box>

            <Box>
              <RichTextEditorForm
                editor={editor}
                label={"Job Description"}
                hForm={hForm}
                name="description"
                setFieldValue={(value: string) => {
                  setValue("description", value);
                }}
              />
            </Box>
            {userDetails && userDetails.isVerified && (
              <Stack gap={40}>
                <Text c="#9E9E9E">
                  Note: Job posting will expire after 30 days
                </Text>
                <Stack
                  bg="#1f3243"
                  p={20}
                  style={{ borderRadius: 15 }}
                  maw={{ base: "100%", sm: 400, md: 600 }}
                >
                  <LabelText label="Job listing type" />
                  <Flex gap={20}>
                    <Button
                      style={{ border: "1px solid #9E9E9E" }}
                      onClick={() => {
                        setValue("isBoosted", false);
                        setValue("subscriptionId", null);
                        clearErrors("isBoosted");
                      }}
                      radius="50px"
                      bg={!isBoosted ? "secondaryGreen.1" : "transparent"}
                      className={classes.secondaryYellowButton}
                      c={!isBoosted ? "black" : "#DFDFDF"}
                    >
                      FREE
                    </Button>
                    <Stack gap={6} w="100%">
                      <Button
                        style={{ border: "1px solid #9E9E9E" }}
                        radius="50px"
                        bg={isBoosted ? "secondaryGreen.1" : "transparent"}
                        onClick={handleBuyPremiumPlan}
                        className={classes.secondaryYellowButton}
                        c={isBoosted ? "black" : "#DFDFDF"}
                        maw={120}
                      >
                        PREMIUM
                      </Button>

                      {boostError && boostError.message && (
                        <Text c="red" fz={12}>
                          {boostError.message}
                        </Text>
                      )}

                      <Text c="#0F793A" fw={600} fz="14">
                        *Get more eyeballs to your job listing and attract top
                        talents
                      </Text>
                    </Stack>
                  </Flex>
                  {subscriptionName && subscriptionId && (
                    <Text my={10} fw={600} c="secondaryYellow.3">
                      Selected Plan :{" "}
                      <span style={{ color: "white" }}>{subscriptionName}</span>
                    </Text>
                  )}
                </Stack>
              </Stack>
            )}
            <Box>
              <PrimaryButton
                label="Post Now"
                fz={{ base: 16, sm: 18 }}
                fw="600"
                px={20}
                py={12}
                w={{ base: "100%", sm: "max-content" }}
                h="max-content"
                onClick={() => {
                  setValue("isPosted", true);
                }}
                type="submit"
              />
              <SecondaryButton
                label="Save as draft"
                fz={{ base: 16, sm: 18 }}
                fw="600"
                px={20}
                py={12}
                mt={{ base: 10, sm: 0 }}
                ml={{ base: 0, sm: 20 }}
                w={{ base: "100%", sm: "max-content" }}
                h="max-content"
                onClick={() => {
                  setValue("isPosted", false);
                }}
                type="submit"
              />
            </Box>
          </Stack>
        </form>
      </SectionContainer>
    </Box>
  );
};
export default JobSeekerJobForm;
