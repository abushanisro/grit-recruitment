import { useApi } from "@/hooks/useApi";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { getAllJobsFromEmployerResponseType } from "@/types/employer";

import ConfirmationModal from "@components/ConfirmationModal";
import CustomModal from "@components/CustomModal";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import ErrorMessage from "@components/ErrorMessage";
import PrimaryButton from "@components/buttons/PrimaryButton";

import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import JobSearchField from "@components/jobs/filters/JobSearchField";

import {
  Box,
  Flex,
  Grid,
  Image,
  Group,
  Stack,
  Text,
  Divider,
  Avatar,
} from "@mantine/core";
import { useDisclosure, useMediaQuery } from "@mantine/hooks";

import { getQueryClient } from "api";
import { contract } from "contract";
import isEmpty from "lodash/isEmpty";
import { Dispatch, SetStateAction, useState } from "react";
import { useForm } from "react-hook-form";
import CustomBadge from "@components/jobs/CustomBadge";
import { getTimeFromNow } from "@/utils/common";
import { useQueryClient } from "@tanstack/react-query";

const sendEmailToastId = "sendEmailToastId";

const SendEmailInviteModal = ({
  opened,
  onClose,
  jobSeekersId,
  setJobSeekersId,
  selectedJobSeekerJobId,
  setSelectedJobSeekerJobId,
  subscriptionId,
}: {
  opened: boolean;
  onClose: () => void;
  jobSeekersId: number[];
  setJobSeekersId: (value: number[]) => void;
  selectedJobSeekerJobId: number | null;
  setSelectedJobSeekerJobId: Dispatch<SetStateAction<number | null>>;
  subscriptionId: string | null;
}) => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  const [loading, setLoading] = useState<boolean>(false);
  const mutateQueryClient = useQueryClient();
  const hForm = useForm<{ searchText: string }>({
    mode: "onChange",
    defaultValues: { searchText: "" },
  });
  const [
    openedSendInviteConfirmation,
    {
      open: onOpenSendInviteConfirmation,
      close: onCloseSendInviteConfirmation,
    },
  ] = useDisclosure(false);
  const { setValue, watch } = hForm;
  const searchText = watch("searchText");
  const { makeApiCall } = useApi();

  const { showToast } = useCustomToast();
  const sendInviteValidation = () => {
    if (!jobSeekersId) {
      showToast({
        status: ToastStatus.warning,
        id: sendEmailToastId,
        message: "Please select a jobseeker to send invite",
      });
      return false;
    }

    if (!selectedJobSeekerJobId) {
      showToast({
        status: ToastStatus.warning,
        id: sendEmailToastId,
        message: "Please select a job to send invite",
      });
      return false;
    }
  };
  const sendInvite = () => {
    // Sometimes the user might click send invite multiple times
    if (jobSeekersId.length === 0) {
      return;
    }
    setLoading(true);
    makeApiCall({
      fetcherFn: async () => {
        const response =
          await getQueryClient().subscription.inviteUsers.mutation({
            body: {
              jobId: Number(selectedJobSeekerJobId),
              jobSeekerIds: jobSeekersId,
              subscriptionId: Number(subscriptionId),
            },
          });
        return response;
      },
      successMsgProps: { message: "Invite sent successfully" },
      onSuccessFn: () => {
        onCloseSendInviteConfirmation();
        setSelectedJobSeekerJobId(null);
        setJobSeekersId([]);
        onClose();
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.subscription.filterDirectory.path],
        });
      },
      finallyFn: () => {
        setLoading(false);
      },
      showFailureMsg: true,
    });
  };
  return (
    <>
      <ConfirmationModal
        opened={openedSendInviteConfirmation}
        onClose={() => {
          setSelectedJobSeekerJobId(null);
          onCloseSendInviteConfirmation();
        }}
        modalHeader={
          jobSeekersId.length == 1
            ? `Are you sure you want to send invite to 1 Jobseeker?`
            : `
        Are you sure you want to send invite to ${jobSeekersId.length} Jobseekers?`
        }
        onSuccessFn={sendInvite}
        loading={loading}
        successButtonLabel={"Confirm"}
      />
      <CustomModal
        opened={opened}
        onClose={() => {
          setSelectedJobSeekerJobId(null);
          setJobSeekersId([]);
          onClose();
        }}
        closeOnClickOutside={true}
        closeOnEscape={true}
        size="90%"
        zIndex={100}
        styles={{
          content: {
            padding: isMobile ? 0 : 20,
            background: "var(--mantine-color-primaryDarkBlue-9)",
            border: "1px solid var(--mantine-color-secondarySkyBlue-4)",
            borderRadius: isMobile ? 10 : 30,
          },
        }}
      >
        <Box pos="relative" mih={500}>
          <Stack pos="sticky" top={0} left={0} right={0} pb={10}>
            <Group justify="space-between">
              <Text c="white" fz={{ base: 20, sm: 32 }} fw={700}>
                Select Job
              </Text>
              <Group gap={40}>
                <Text fz={{ sm: 20 }} fw={600} c="secondaryGreen.1">
                  {jobSeekersId.length}{" "}
                  {jobSeekersId.length == 1 ? "JobSeeker" : "jobseekers"}
                </Text>
                <PrimaryButton
                  label="Send"
                  px={30}
                  onClick={() => {
                    if (!jobSeekersId || !selectedJobSeekerJobId) {
                      sendInviteValidation();
                      return;
                    }
                    onOpenSendInviteConfirmation();
                  }}
                />
              </Group>
            </Group>
            <JobSearchField
              name="searchText"
              hForm={hForm}
              onReset={() => setValue("searchText", "")}
            />
          </Stack>
          <JobSeekerJobsList
            searchText={searchText}
            setSelectedJobSeekerJobId={setSelectedJobSeekerJobId}
            selectedJobSeekerJobId={selectedJobSeekerJobId}
          />
        </Box>
      </CustomModal>
    </>
  );
};

const JobSeekerJobsList = ({
  searchText,
  selectedJobSeekerJobId,
  setSelectedJobSeekerJobId,
}: {
  searchText: string;
  selectedJobSeekerJobId: number | null;
  setSelectedJobSeekerJobId: (value: number | null) => void;
}) => {
  const { data, isLoading, error } =
    getQueryClient().subscription.getAllJobsFromEmployer.useQuery(
      [contract.subscription.getAllJobsFromEmployer.path, searchText],
      {
        query: { searchText: searchText, pageSize: "20", pageNumber: "1" },
      }
    );
  if (isLoading) {
    return <JobListSkeletonCard />;
  }
  if (error) {
    return <ErrorMessage error={error} />;
  }

  if (isEmpty(data.body)) {
    return <CustomErrorMessage errorMessage="No jobs found" />;
  }

  const handleSelected = (
    eachJobSeekerJob: getAllJobsFromEmployerResponseType
  ) => {
    if (selectedJobSeekerJobId === eachJobSeekerJob.id) {
      setSelectedJobSeekerJobId(null);
      return;
    }

    setSelectedJobSeekerJobId(eachJobSeekerJob.id);
  };
  return (
    <Box style={{ overflow: "auto" }} h={400}>
      {data.body.results.map((eachJobSeekerJob, index) => {
        return (
          <Box
            key={index}
            onClick={() => handleSelected(eachJobSeekerJob)}
            style={{ cursor: "pointer" }}
          >
            <JobSeekerJobCard
              data={eachJobSeekerJob}
              isSelected={selectedJobSeekerJobId === eachJobSeekerJob.id}
            />
          </Box>
        );
      })}
    </Box>
  );
};

const JobSeekerJobCard = ({
  data,
  isSelected,
}: {
  data: getAllJobsFromEmployerResponseType;
  isSelected: boolean;
}) => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  const isLaptop = useMediaQuery("(max-width: 1024px)");
  const isTablet = useMediaQuery("(max-width: 768px)");
  return (
    <Stack
      pos="relative"
      bg={
        isSelected
          ? "transparent"
          : "linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
      }
      ml={{ base: 10, md: 0 }}
      mt={{ base: 40, md: 58 }}
      mb={58}
      px={{ base: 20, sm: 40 }}
      py={{ base: 20, sm: 40 }}
      style={{
        borderRadius: isMobile ? 10 : 38,
        border: "1px solid",
        borderColor: isSelected
          ? "var(--mantine-color-primarySkyBlue-6)"
          : "var(--mantine-color-secondaryGreen-1)",
        boxShadow: isSelected
          ? "0px 4px 4px 0px var(--mantine-color-primarySkyBlue-6)"
          : "0px 4px 4px 0px var(--mantine-color-secondaryGreen-1)",
      }}
      gap={isLaptop ? 20 : 40}
    >
      <Grid gutter={{ base: "md", sm: "xl" }}>
        <Grid.Col span={{ base: 4, sm: "content" }}>
          <Avatar
            w={{ base: 70, sm: 100, md: 120 }}
            h={{ base: 70, sm: 100, md: 120 }}
            bg="customGray.2"
            style={{
              border: "1px solid",
              borderColor: "var(--mantine-color-secondaryGreen-1)",
              borderRadius: "50%",
            }}
            src={data.company.logo}
            alt="company logo"
          />
        </Grid.Col>
        <Grid.Col span={{ base: 8, sm: "auto" }} py={0}>
          <Stack gap={0} justify="center" h="100%">
            <Flex gap={isMobile ? 7 : 14}>
              <Image
                src="/images/companyLogo.svg"
                alt="logo"
                w={isLaptop ? 14 : 18}
                h={isLaptop ? 14 : 18}
              />
              <Divider
                w={2}
                h={16}
                orientation="vertical"
                color="secondaryGreen.1"
              />
              <Text
                tt="uppercase"
                c="secondaryGreen.1"
                fw="400"
                lh="1.17"
                lts={2}
                fz={{ base: 14, md: 16, lg: 20, xl: 20 }}
                mb={{ base: 10, xl: 12 }}
              >
                {data.company.name}
              </Text>
            </Flex>
            <Text
              tt="uppercase"
              c="secondaryGreen.1"
              fw="400"
              lh="1.17"
              lts={2}
              fz={{ base: 14, md: 16, lg: 20, xl: 20 }}
              mb={{ base: 10, xl: 12 }}
            >
              {data.title}
            </Text>
          </Stack>
        </Grid.Col>
      </Grid>
      {data.company.updatedAt && (
        <CustomBadge
          label={`posted ${getTimeFromNow(data.company.updatedAt)}`}
          pos="absolute"
          right={{ base: 20, sm: 40 }}
          top={-31}
          px={12}
          py={14}
          style={{
            borderRadius: "10px 10px 0px 0px",
          }}
        />
      )}
      <Grid justify="space-between" align="center">
        <Grid.Col span={{ base: 12, sm: 8 }}>
          <Text
            tt="capitalize"
            c="white"
            style={{ borderRadius: 12 }}
            fz={{ base: 14, md: 16, lg: 20, xl: 20 }}
            lh="1.17"
            px={{ base: 12, sm: 20 }}
            py={{ base: 7, sm: 16 }}
            fw={500}
            bg="primaryPaleBlue.9"
            maw={{ base: 550 }}
            pos="relative"
          >
            {`min experience  ${data.minExperienceInYears} years required`}
          </Text>
        </Grid.Col>
      </Grid>

      {isSelected && (
        <Flex
          pos="absolute"
          top={isTablet ? 0 : 30}
          right={isTablet ? 20 : 60}
          gap={10}
          my={{ base: 30, lg: 10, xl: 10 }}
        >
          <Image src="/images/successIcon.svg" alt="logo" w={26} h={25} />
          <Text display={isTablet ? "none" : "block"} c={"white"} fz={"16"}>
            Selected
          </Text>
        </Flex>
      )}
    </Stack>
  );
};
export default SendEmailInviteModal;
