import {
  Avatar,
  Flex,
  Group,
  Stack,
  Text,
  Image,
  Box,
  Grid,
} from "@mantine/core";
import CustomBadge from "@/components/jobs/CustomBadge";
import { Fragment } from "react";
import IconWithLabel from "@/components/jobs/jobcard/IconWithLabel";
import { getColor } from "@/utils/colors";
import { getTimeFromNow } from "@/utils/common";
import { ManageJobsPaginatedListType } from "@/types/jobs";
import isEmpty from "lodash/isEmpty";
import { useApi } from "@/hooks/useApi";
import { getQueryClient } from "api";
import { useQueryClient } from "@tanstack/react-query";
import { contract } from "contract";
import dynamic from "next/dynamic";
import { useDisclosure, useMediaQuery } from "@mantine/hooks";
import ManageJobsBadge from "@/components/jobs/manageJobs/ManageJobsBadge";
import { defaultJobSeekerApplicantFilters } from "pages/manageJobSeekerApplicants";
import CustomToolTip from "@components/CustomToolTip";
import { IconBolt } from "@tabler/icons-react";

import JobListSkeletonCard from "@components/JobListSkeletonCard";
import ErrorMessage from "@components/ErrorMessage";

import { useUserData } from "@/contexts/UserProvider";

import ManageJobsButtonComponent from "@components/ManageJobsButtonComponent";
import ExperienceYearsComponent from "@components/ExperienceYearsComponent";
import { PostPaidActiveSubscriptionResponseType } from "@/types/postPaid";

const PostPaidJobSeekerCardModal = dynamic(
  () => import("@components/PostPaidJobSeekerCardModal")
);
const UnPromotedModal = dynamic(() => import("@components/UnPromotedModal"));

const ConfirmationModal = dynamic(
  () => import("@components/ConfirmationModal")
);

const JobSeekerJobsCard = ({
  jobSeekerData,
}: {
  jobSeekerData: ManageJobsPaginatedListType["results"][number];
}) => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  const isTablet = useMediaQuery("(max-width: 768px)");
  const [opened, { open, close }] = useDisclosure(false);
  const [postJobOpened, { open: postJobOpen, close: postJobClose }] =
    useDisclosure(false);
  const [boostJobOpened, { open: boostJobOpen, close: boostJobClose }] =
    useDisclosure(false);
  const [
    unPromotedJobOpened,
    { open: unPromoteJobOpen, close: unPromoteJobClose },
  ] = useDisclosure(false);
  const { makeApiCall } = useApi();
  const mutateQueryClient = useQueryClient();
  const isJobInDraftStatus = !jobSeekerData.isPosted;
  const isJobPendingStatus =
    !jobSeekerData.isAdminApproved && jobSeekerData.isPosted;
  const isJobPosted =
    jobSeekerData.isAdminApproved &&
    jobSeekerData.isPosted &&
    jobSeekerData.adminApprovedTime;
  const isPromoted = jobSeekerData.isBoosted;
  const { userDetails } = useUserData();

  const isJobBoostPurchaseActive =
    userDetails && userDetails.isJobBoostPurchaseActive;
  const queryObj = {
    pageSize: String(5),
    pageNumber: String(1),
  };

  const { data, isLoading, error } =
    getQueryClient().paidJobs.getActiveSubscriptions.useQuery(
      [contract.paidJobs.getActiveSubscriptions.path, queryObj],
      {
        query: queryObj,
      },
      {
        refetchInterval: 5000,
      }
    );

  const activeSubscriptionData =
    data?.body.results.filter(
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
  const getPostStatus = () => {
    if (isJobInDraftStatus) {
      return (
        <Flex gap={10} my={{ base: 30, lg: 10, xl: 10 }}>
          <Image
            src="/images/informative.svg"
            alt="logo"
            w={20}
            h={20}
            style={{ opacity: "0.8" }}
          />
          <Text
            fz={14}
            c={"white"}
            style={{ filter: "brightness(60%)" }}
            maw={{ base: 440, lg: 600 }}
          >
            To go live with this jobpost you need to post your job for admin to
            approve
          </Text>
        </Flex>
      );
    }
    if (isJobPendingStatus) {
      return (
        <Flex gap={10} my={{ base: 30, lg: 10, xl: 10 }}>
          <Image
            src="/images/informative.svg"
            alt="logo"
            w={20}
            h={20}
            style={{ opacity: "0.8" }}
          />
          <Text fz={14} c={"white"} style={{ filter: "brightness(60%)" }}>
            Admin is currently reviewing your jobs
          </Text>
        </Flex>
      );
    }
    if (isJobPosted && jobSeekerData.adminApprovedTime) {
      return (
        <Flex gap={10} my={{ base: 30, lg: 10, xl: 10 }}>
          <Image
            src="/images/informative.svg"
            alt="logo"
            w={20}
            h={20}
            style={{ opacity: "0.8" }}
          />
          <Text fz={14} c={"white"} style={{ filter: "brightness(60%)" }}>
            your job is now live
          </Text>
        </Flex>
      );
    }
    return "";
  };
  const getJobPostedTimeLabel = () => {
    if (isJobInDraftStatus) {
      return "Draft";
    }
    if (isJobPendingStatus) {
      return "Pending";
    }
    if (isJobPosted && jobSeekerData.adminApprovedTime) {
      return `posted ${getTimeFromNow(jobSeekerData.adminApprovedTime)}`;
    }
    return "";
  };
  const handleDelete = () => {
    makeApiCall({
      fetcherFn: async () => {
        const response =
          await getQueryClient().job.deleteJobAsEmployer.mutation({
            body: {
              jobId: jobSeekerData.id,
            },
          });
        return response;
      },
      successMsgProps: { message: "Job deleted successfully" },
      onSuccessFn: () => {
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.job.fetchJobsByEmployer.path],
        });
        close();
      },
      showFailureMsg: true,
    });
  };
  const handlePostJob = () => {
    makeApiCall({
      fetcherFn: async () => {
        const response = await getQueryClient().job.changeIsPost.mutation({
          body: {
            jobId: jobSeekerData.id,
          },
        });
        return response;
      },
      successMsgProps: { message: "Job posted successfully" },
      onSuccessFn: () => {
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.job.fetchJobsByEmployer.path],
        });
        postJobClose();
      },
      showFailureMsg: true,
    });
  };

  const handlePromoteJob = () => {
    if (!isJobBoostPurchaseActive || activeSubscriptionData.length === 0) {
      unPromoteJobOpen();
    } else {
      boostJobOpen();
    }
  };

  return (
    <>
      <PostPaidJobSeekerCardModal
        activeSubscriptionData={activeSubscriptionData}
        jobId={jobSeekerData.id}
        opened={boostJobOpened}
        onClose={boostJobClose}
      />
      <UnPromotedModal
        opened={unPromotedJobOpened}
        onClose={unPromoteJobClose}
      />

      <ConfirmationModal
        opened={opened}
        onClose={close}
        modalHeader={"Are you sure you want to delete this job?"}
        onSuccessFn={handleDelete}
        successButtonLabel={"Delete job"}
      />
      <ConfirmationModal
        opened={postJobOpened}
        onClose={postJobClose}
        modalHeader={"Are you sure you want to post this job?"}
        onSuccessFn={handlePostJob}
        successButtonLabel={"Post job"}
      />
      <Group
        pos="relative"
        bg={isPromoted ? "secondaryDarkBlue.9" : "primaryDarkBlue.9"}
        mb={{ base: 24, md: 32 }}
        mx={10}
        p={{ base: 24, md: 32, lg: 50 }}
        style={{
          borderRadius: isMobile ? 10 : 20,
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryGreen-1)",
          boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)",
        }}
        align={"flex-end"}
        justify={"space-between"}
      >
        {isPromoted && (
          <Stack
            gap={0}
            pos={"absolute"}
            top={isTablet ? "60px" : "20px"}
            right={isTablet ? "10px" : isJobPosted ? "180px" : "100px"}
          >
            <Flex gap={6} c="secondaryGreen.1">
              <CustomToolTip
                label={`Job is Promoted, ${jobSeekerData.boostedDaysLeft} days boost left`}
              >
                <IconBolt />
              </CustomToolTip>

              <Text
                display={{ base: "none", sm: "block" }}
                fz={{ base: 10, sm: 14 }}
              >
                {" "}
                Promoted Job
              </Text>
            </Flex>
            <Text
              display={{ base: "none", sm: "block" }}
              ml={30}
              c="secondaryGreen.1"
              fz={{ base: 10, sm: 14 }}
            >
              {jobSeekerData.boostedDaysLeft} days left
            </Text>
          </Stack>
        )}
        <Stack gap={0}>
          <Avatar
            pos="absolute"
            w={{ base: 40, md: 50, lg: 70 }}
            h={{ base: 40, md: 50, lg: 70 }}
            left={{ base: -20, md: -25, lg: -35 }}
            bg="customGray.2"
            style={{
              border: "1px solid",
              borderColor: "var(--mantine-color-primaryGreen-3)",
            }}
            src={jobSeekerData.companyLogo ?? ""}
            alt="company logo"
          />
          {isJobPosted ? (
            <ManageJobsBadge
              label={getJobPostedTimeLabel()}
              style={{
                borderBlock: "1px solid",
                borderLeft: "1px solid",
                borderRight: "0",
                borderColor: "var(--mantine-color-primarySkyBlue-6)",
                borderTopRightRadius: 0,
                borderBottomRightRadius: 0,
              }}
              c="primarySkyBlue.6"
            />
          ) : (
            <ManageJobsBadge
              label={getJobPostedTimeLabel()}
              style={{
                borderTopRightRadius: 0,
                borderBottomRightRadius: 0,
              }}
            />
          )}

          <Text
            tt="uppercase"
            c="white"
            fw="400"
            lh="1.17"
            lts={4}
            fz={{ base: 12, sm: 16, xl: 18 }}
            mb={{ base: 10, xl: 12 }}
            mt={{ base: 30, md: 0 }}
            maw={{ base: "80%", sm: "70%" }}
            style={{ wordBreak: "break-word" }}
            lineClamp={4}
          >
            {jobSeekerData.companyName}
          </Text>
          <Text
            tt="capitalize"
            c="white"
            fw={700}
            fz={{ base: 18, sm: 20, xl: 28 }}
            lh="1.17"
            mb={{ base: 10, sm: 12, xl: 16 }}
          >
            {jobSeekerData.title}
          </Text>
          <Flex style={{ gap: 10 }} wrap={"wrap"} mb={28}>
            {[
              jobSeekerData.industries
                .map((eachIndustry) => eachIndustry.name)
                .join(", "),
              jobSeekerData.subFunctions
                .map((eachSubFction) => eachSubFction.name)
                .join(", "),
              jobSeekerData.employmentModes.join(", "),
            ]
              .filter((eachValue) => eachValue)
              .map((jobSeekerData: string, index: number) => {
                return (
                  <Fragment key={index}>
                    <Box
                      onClick={(e) => {
                        e.stopPropagation();
                        e.preventDefault();
                      }}
                    >
                      <CustomToolTip label={jobSeekerData}>
                        <CustomBadge
                          label={jobSeekerData}
                          c={getColor(index)}
                          fz={{ base: 12, md: 14 }}
                          maw={200}
                          style={{ textOverflow: "ellipsis" }}
                        />
                      </CustomToolTip>
                    </Box>
                  </Fragment>
                );
              })}
          </Flex>

          <Box>
            <Group mb={12} gap={isMobile ? 16 : 30} wrap="wrap">
              {!isEmpty(jobSeekerData.cities) && (
                <IconWithLabel
                  icon={
                    <Image
                      src="/images/location.svg"
                      alt="location"
                      w={{ base: 16, md: 18 }}
                      h={{ base: 16, md: 18 }}
                    />
                  }
                  label={jobSeekerData.cities
                    .map((eachCity) => eachCity.name)
                    .join(", ")}
                />
              )}
              {!isEmpty(jobSeekerData.employmentTypes) && (
                <IconWithLabel
                  icon={
                    <Image
                      src="/images/clock.svg"
                      alt="time"
                      w={{ base: 16, md: 19 }}
                      h={{ base: 16, md: 19 }}
                    />
                  }
                  label={jobSeekerData.employmentTypes.join(", ")}
                />
              )}
              {(jobSeekerData.minCtc || jobSeekerData.maxCtc) && (
                <IconWithLabel
                  icon={
                    <Image
                      src="/images/moneyBag.svg"
                      alt="money"
                      w={{ base: 16, md: 19 }}
                      h={{ base: 16, md: 19 }}
                    />
                  }
                  label={`${[
                    jobSeekerData.minCtc ?? "",
                    jobSeekerData.maxCtc ?? "",
                  ]
                    .filter((eachValue) => eachValue)
                    .join("-")} LPA`}
                />
              )}
            </Group>
            <ExperienceYearsComponent
              years={{
                minExp: jobSeekerData.minExp,
                maxExp: jobSeekerData.maxExp,
              }}
            />
          </Box>
        </Stack>

        <Group w="100%" justify="space-between">
          {getPostStatus()}
          <ManageJobsButtonComponent
            isJobPendingStatus={isJobPendingStatus}
            isPromoted={isPromoted}
            isJobInDraftStatus={isJobInDraftStatus}
            isJobPosted={isJobPosted}
            open={open}
            href={`/manageJobSeekerApplicants/?jobId=${
              jobSeekerData.id
            }&filters=${JSON.stringify({
              ...defaultJobSeekerApplicantFilters,
            })}`}
            handlePromoteJob={handlePromoteJob}
            postJobOpen={postJobOpen}
          />
        </Group>
      </Group>
    </>
  );
};

export default JobSeekerJobsCard;
