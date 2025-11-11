import {
  Box,
  Flex,
  Group,
  Image,
  Center,
  Grid,
  Menu,
  Stack,
  Text,
} from "@mantine/core";
import { Fragment, useEffect, useState } from "react";
import { getQueryClient } from "api";
import { UseFormReturn } from "react-hook-form";
import isEmpty from "lodash/isEmpty";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import { contract } from "contract";
import ErrorMessage from "@components/ErrorMessage";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import dynamic from "next/dynamic";
import { useDisclosure, useMediaQuery } from "@mantine/hooks";
import CustomPagination from "@components/CustomPagination";
import JobSeekerDesktopCard from "@/components/jobSeekerDirectory/JobSeekerDesktopCard";
import JobSeekerCard from "@components/jobSeekerDirectory/JobSeekerCard";
import classes from "styles/menu.module.css";
import {
  JobseekerDirectoryFilterInterface,
  yesOrNoEnum,
} from "@/types/jobSeeker";
import { useLogin } from "@/contexts/LoginProvider";
import { ExperienceInYear, NoticePeriodEnum } from "contract/enum";
import isNil from "lodash/isNil";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import EmployerOnlyInviteButton from "@components/buttons/EmployerOnlyInviteButton";
import CommonCheckbox from "@components/CommonCheckbox";
import CustomModal from "@components/CustomModal";
import RenewJobseekerSubscriptionModal from "./RenewJobseekerSubscriptionModal";
import { useRouter } from "next/router";
import { isMobile } from "react-device-detect";

const SendEmailInviteModal = dynamic(
  () => import("@/components/jobSeekerDirectory/JobSeekerEmailInviteModal")
);

const JobSeekerDirectoryContainer = ({
  hForm,
}: {
  hForm: UseFormReturn<JobseekerDirectoryFilterInterface>;
}) => {
  const { watch, setValue } = hForm;

  const [jobSeekersId, setJobSeekersId] = useState<number[]>([]); // Initial state
  const isTablet = useMediaQuery("(max-width: 768px)");

  const pageNumber = watch("pageNumber");
  const pageSize = watch("pageSize");
  const searchText = watch("searchText");
  const skills = watch("skills");
  const noticePeriod = watch("noticePeriod");
  const experience = watch("experience");
  const portfolio = watch("portfolio");
  const lastLogin = watch("lastLogin");
  const ctc = watch("ctc");
  const lastToDate = watch("customDate");
  const languageIds = watch("languageIds");
  const activeSubscription = watch("activeSubscription");

  const [
    openedEmailInvite,
    { open: onEmailInviteOpen, close: onEmailInviteClose },
  ] = useDisclosure(false);

  const [
    openedRenewSubscription,
    { open: onRenewSubscriptionOpen, close: onRenewSubscriptionClose },
  ] = useDisclosure(false);

  const [selectedJobSeekerJobId, setSelectedJobSeekerJobId] = useState<
    number | null
  >(null);

  const location = watch("locationIds");
  const preferredLocations = watch("preferredLocations");
  const { isLoggedIn } = useLogin();
  const { showToast } = useCustomToast();
  const router = useRouter();

  useEffect(() => {
    setJobSeekersId([]);
  }, [pageNumber]);

  const queryObj = {
    pageSize: String(pageSize),
    pageNumber: String(pageNumber),
    searchText: searchText,

    skillIds: skills
      ? skills.map((eachValue) => {
          return eachValue.value;
        })
      : undefined,
    locationIds: location
      ? location.map((eachValue) => {
          return eachValue.value;
        })
      : undefined,
    languageIds: languageIds
      ? languageIds.map((eachValue) => {
          return eachValue.value;
        })
      : undefined,
    preferredLocations: preferredLocations
      ? preferredLocations.map((eachValue) => {
          return eachValue.value;
        })
      : undefined,
    noticePeriod: noticePeriod
      ? noticePeriod.map((period) => period.value as NoticePeriodEnum)
      : undefined,

    experience: experience ? (experience.value as ExperienceInYear) : undefined,
    isPortfolioAvailable: portfolio
      ? portfolio.value === yesOrNoEnum.yes
      : undefined,
    lastLoginFromValue: lastLogin
      ? lastLogin.value === "custom"
        ? lastToDate && !isNil(lastToDate[0])
          ? String(lastToDate[0])
          : undefined
        : lastLogin.value
      : undefined,
    minCTC:
      ctc && ctc.value && ctc.value.split("-")[0]
        ? ctc?.value.split("-")[0]
        : undefined,
    maxCTC:
      ctc && ctc.value && ctc.value.split("-")[1]
        ? ctc.value.split("-")[1]
        : undefined,
    lastLoginToValue:
      lastLogin &&
      lastLogin.value === "custom" &&
      lastToDate &&
      !isNil(lastToDate[1])
        ? String(lastToDate[1])
        : undefined,
  };

  const { data, isLoading, error } =
    getQueryClient().subscription.filterDirectory.useQuery(
      [contract.subscription.filterDirectory.path, queryObj],
      { query: queryObj }
    );

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

  if (isNil(activeSubscription)) {
    return (
      <CustomErrorMessage errorMessage="Please Select a subscription plan in the filter" />
    );
  }

  if (isEmpty(data.body.results)) {
    return <CustomErrorMessage errorMessage="No Jobseekers found" />;
  }

  return (
    <>
      <SendEmailInviteModal
        opened={openedEmailInvite}
        onClose={onEmailInviteClose}
        jobSeekersId={jobSeekersId}
        setJobSeekersId={setJobSeekersId}
        selectedJobSeekerJobId={selectedJobSeekerJobId}
        setSelectedJobSeekerJobId={setSelectedJobSeekerJobId}
        subscriptionId={activeSubscription?.value}
      />
      <CustomModal
        styles={{
          content: {
            background: "black",
            border: "1px solid var(--mantine-color-secondarySkyBlue-4)",
            borderRadius: isMobile ? 10 : 30,
          },
        }}
        closeOnClickOutside={true}
        opened={openedRenewSubscription}
        onClose={onRenewSubscriptionClose}
      >
        <RenewJobseekerSubscriptionModal
          handleClick={() => router.push("/subscription")}
        />
      </CustomModal>
      <Stack my={{ base: 40, md: 30 }} gap={33}>
        <Flex justify="space-between">
          <Flex gap={6}>
            <CommonCheckbox
              checked={
                jobSeekersId.length == data.body.results.length ? true : false
              }
              onChange={(e) => {
                if (e.target.checked) {
                  const select = data.body.results.map((data) => data.id);
                  setJobSeekersId(select);
                } else {
                  setJobSeekersId([]);
                }
              }}
            />

            <Menu
              width="target"
              styles={{
                dropdown: {
                  borderColor: "var(--mantine-color-primarySkyBlue-6)",
                },
              }}
            >
              <Menu.Target>
                <Group
                  px="16"
                  style={{ borderRadius: "4px", cursor: "pointer" }}
                  h={22}
                  justify="center"
                  bg="customBlack.5"
                >
                  <Text fz={{ base: 12, sm: 14 }} fw={400} lh="1.17">
                    {jobSeekersId.length} Selected
                  </Text>
                  <Image src="/images/dropDownPurple.svg" alt="verified" />
                </Group>
              </Menu.Target>

              <Menu.Dropdown bg="primaryDarkBlue.9">
                <Menu.Item
                  c="white"
                  className={classes.menuBar}
                  fz={{ base: 14 }}
                  onClick={() => {
                    const selectAll = data.body.results.map((data) => data.id);
                    setJobSeekersId(selectAll);
                  }}
                >
                  Select all
                </Menu.Item>
                <Menu.Item
                  className={classes.menuBar}
                  c="white"
                  fz={{ base: 14 }}
                  onClick={() => {
                    setJobSeekersId([]);
                  }}
                >
                  Clear section
                </Menu.Item>
              </Menu.Dropdown>
            </Menu>
          </Flex>
          <Box>
            {isLoggedIn && (
              <EmployerOnlyInviteButton
                onEmployerSendInvite={() => {
                  if (isEmpty(jobSeekersId)) {
                    showToast({
                      status: ToastStatus.warning,
                      message: "Please select a profile to send invite",
                    });
                    return;
                  }
                  onEmailInviteOpen();
                }}
              />
            )}
          </Box>
        </Flex>

        <>
          {data.body.results.map((eachJobSeekers, index: number) => {
            return (
              <Fragment key={index}>
                {isTablet ? (
                  <>
                    <JobSeekerCard
                      setJobSeekersId={setJobSeekersId}
                      jobSeekersId={jobSeekersId}
                      onEmailInviteOpen={onEmailInviteOpen}
                      onRenewSubscriptionOpen={onRenewSubscriptionOpen}
                      data={eachJobSeekers}
                      subscriptionId={activeSubscription?.value}
                    />
                  </>
                ) : (
                  <JobSeekerDesktopCard
                    data={eachJobSeekers}
                    setJobSeekersId={setJobSeekersId}
                    onEmailInviteOpen={onEmailInviteOpen}
                    onRenewSubscriptionOpen={onRenewSubscriptionOpen}
                    jobSeekersId={jobSeekersId}
                    subscriptionId={activeSubscription?.value}
                  />
                )}
              </Fragment>
            );
          })}
        </>
      </Stack>

      <Center>
        <CustomPagination
          value={pageNumber}
          onChange={(e) => {
            setValue("pageNumber", e);
          }}
          total={data.body.totalPages}
        />
      </Center>
    </>
  );
};

export default JobSeekerDirectoryContainer;
