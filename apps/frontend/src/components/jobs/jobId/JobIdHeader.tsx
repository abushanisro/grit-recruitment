import {
  ActionIcon,
  Image,
  Paper,
  Box,
  Text,
  Flex,
  Anchor,
} from "@mantine/core";
import { useDisclosure, useMediaQuery } from "@mantine/hooks";
import { getQueryClient } from "api";
import { JobIdResultsType } from "@/types/jobs";
import { useLogin } from "@/contexts/LoginProvider";
import { getApiUrl } from "@/env";
import { useApi } from "@/hooks/useApi";
import { useQueryClient } from "@tanstack/react-query";
import { contract } from "contract";
import JobApplyButton from "@components/jobs/JobApplyButton";
import dynamic from "next/dynamic";
import { useEffect } from "react";
import { useRouter } from "next/router";
import { useUserData } from "@/contexts/UserProvider";
import { UserRole } from "contract/enum";
import { appendQueryToRedirectUrl } from "@/utils/common";

const ApplyJobModal = dynamic(
  () => import("@components/jobs/jobId/ApplyJobModal")
);

const JobIdHeader = ({
  id,
  data,
}: {
  id: string;
  data: { body: JobIdResultsType };
}) => {
  const { isLoggedIn } = useLogin();
  const router = useRouter();
  const { pathname, query } = router;
  const isMobile = useMediaQuery("(max-width: 425px)");
  const { makeApiCall } = useApi();
  const mutateQueryClient = useQueryClient();
  const [opened, { open, close }] = useDisclosure(false);
  const { userDetails } = useUserData();
  const saveJobAfterLogin = String(query.saveJobAfterLogin ?? "");
  const applyJobModalOpen = String(query.applyJobModalOpen ?? "");

  const applyForJob = () => {
    if (!isLoggedIn) {
      window.open(
        `${getApiUrl()}/google?redirectUrl=${encodeURIComponent(
          appendQueryToRedirectUrl({
            url: window.location.href,
            query: "applyJobModalOpen",
            queryValue: "true",
          })
        )}`,
        "_self"
      );
      return;
    }
    open();
  };
  const saveJob = ({ jobId }: { jobId: number }) => {
    makeApiCall({
      fetcherFn: async () => {
        const response = await getQueryClient().job.saveJob.mutation({
          body: {
            jobId: Number(jobId),
          },
        });
        return response;
      },
      successMsgProps: { message: "Job saved successfully" },
      onSuccessFn: () => {
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.job.getJobById.path],
        });
      },
      showFailureMsg: true,
    });
  };
  const unsaveJob = ({ jobId }: { jobId: number }) => {
    makeApiCall({
      fetcherFn: async () => {
        const response = await getQueryClient().job.unsaveJob.mutation({
          body: {
            jobId: Number(jobId),
          },
        });
        return response;
      },
      successMsgProps: { message: "Job unsaved successfully" },
      onSuccessFn: () => {
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.job.getJobById.path],
        });
      },
      showFailureMsg: true,
    });
  };
  useEffect(() => {
    if (!router.isReady || !data || !saveJobAfterLogin) {
      return;
    }
    if (
      isLoggedIn &&
      userDetails &&
      userDetails.role === UserRole.jobSeeker &&
      saveJobAfterLogin === "true" &&
      data.body.isSaved === false
    ) {
      saveJob({ jobId: data.body.id });
      delete query.saveJobAfterLogin;
      router.replace({ pathname, query }, undefined, { shallow: true });
    }
  }, [router.isReady, saveJobAfterLogin, data]);
  useEffect(() => {
    if (!router.isReady || !data || !applyJobModalOpen) {
      return;
    }
    if (
      isLoggedIn &&
      userDetails &&
      userDetails.role === UserRole.jobSeeker &&
      applyJobModalOpen === "true" &&
      !data.body.isApplied &&
      !data.body.isExternalJob
    ) {
      open();
      delete query.applyJobModalOpen;
      router.replace({ pathname, query }, undefined, { shallow: true });
    }
  }, [router.isReady, applyJobModalOpen, data]);
  return (
    <Paper
      h={{ base: 240, sm: 271 }}
      style={{
        borderTopLeftRadius: isMobile ? 10 : 38,
        borderTopRightRadius: isMobile ? 10 : 38,
        borderBottomLeftRadius: 0,
        borderBottomRightRadius: 0,
      }}
      bg="primaryGrey.1"
      pl={{ base: 30, sm: 50, md: 100, xl: 131 }}
      pr={{ base: 30, sm: 54 }}
      py={54}
    >
      <ApplyJobModal
        opened={opened}
        onClose={close}
        title={data.body.companyName}
        designation={data.body.designation}
        logoUrl={data.body.companyLogo}
        jobId={id}
      />

      <Flex
        direction={{ base: "column-reverse", sm: "row" }}
        justify={{ base: "flex-end", sm: "flex-end" }}
        align={{ base: "flex-end" }}
        gap={10}
      >
        {data.body.isExternalJob ? (
          <Anchor href={data.body.externalLink ?? ""} target="_blank">
            <JobApplyButton
              label="Apply Job Externally"
              radius={isMobile ? 6 : 10}
            />
          </Anchor>
        ) : (
          <JobApplyButton
            label={!data.body.isApplied ? "Apply Now" : "Applied"}
              onClick={applyForJob}
              style={{zIndex:10}}
            radius={isMobile ? 6 : 10}
            disabled={data.body.isApplied}
          />
        )}

        <Box pos="relative">
          <ActionIcon
            p={{ base: 16, sm: 20 }}
            variant="outline"
            style={{
              borderRadius: 8,
              borderColor: "var(--mantine-color-secondaryGreen-1)",
            }}
            h={{ base: 16, sm: 40, xl: 50 }}
            w={{ base: 16, sm: 40, xl: 50 }}
            onClick={() => {
              if (!isLoggedIn) {
                window.open(
                  `${getApiUrl()}/google?redirectUrl=${encodeURIComponent(
                    appendQueryToRedirectUrl({
                      url: window.location.href,
                      query: "saveJobAfterLogin",
                      queryValue: "true",
                    })
                  )}`,
                  "_self"
                );
                return;
              }
              if (!data.body.isSaved) {
                saveJob({ jobId: data.body.id });
                return;
              }
              unsaveJob({ jobId: data.body.id });
            }}
          >
            {!data.body.isSaved ? (
              <Image
                src="/images/bookmarkUnfilled.svg"
                alt="bookmark"
                w={{ base: 14, sm: 20, xl: 24 }}
                h={{ base: 14, sm: 20, xl: 24 }}
              />
            ) : (
              <Image
                src="/images/bookmark.svg"
                alt="bookmark"
                w={{ base: 14, sm: 20, xl: 24 }}
                h={{ base: 14, sm: 20, xl: 24 }}
              />
            )}
          </ActionIcon>
          <Image
            w={{ base: 70, md: 100 }}
            h={{ base: 70, md: 100 }}
            pos="absolute"
            display={{ base: "none", sm: "block" }}
            right={5}
            top={60}
            src="/images/saveJobsForLaterDesktop.svg"
            alt="info"
          />
        </Box>
      </Flex>
      <Flex h="100%" align="flex-end" pb={{ base: 30, sm: 10 }}>
        <Text
          tt="capitalize"
          fz={{ base: 20, sm: 34, xl: 34 }}
          fw={700}
          lh="1.17"
          lineClamp={2}
          maw={{ base: 300, sm: 400, md: 540 }}
        >
          {data.body.designation}
        </Text>
      </Flex>
    </Paper>
  );
};
export default JobIdHeader;
