import {
  Avatar,
  Flex,
  Group,
  Stack,
  Text,
  Image,
  Grid,
  Box,
} from "@mantine/core";
import CustomBadge from "@/components/jobs/CustomBadge";
import { Fragment } from "react";
import IconWithLabel from "@/components/jobs/jobcard/IconWithLabel";
import { getColor } from "@/utils/colors";
import isNil from "lodash/isNil";
import { getTimeFromNow } from "@/utils/common";
import { JobPaginatedListType } from "@/types/jobs";
import CustomButton from "@components/buttons/CustomButton";
import { useLogin } from "@/contexts/LoginProvider";
import { getQueryClient } from "api";
import isEmpty from "lodash/isEmpty";
import { getApiUrl } from "@/env";
import { useQueryClient } from "@tanstack/react-query";
import { useApi } from "@/hooks/useApi";
import { contract } from "contract";
import { useMediaQuery } from "@mantine/hooks";
import { useHover } from "@mantine/hooks";
import { generateSlugForIdPagePath } from "contract/utils";
import Link from "next/link";
import CustomToolTip from "@components/CustomToolTip";
import ExperienceYearsComponent from "@components/ExperienceYearsComponent";

export const ExperienceText = ({
  expeienceInYears,
}: {
  expeienceInYears: number;
}) => {
  return (
    <Text fz={{ base: 12, sm: 14, md: 16, xl: 18 }} lh="1.17" c="white">
      Years of Experience:{" "}
      <Text span fw={700} fz="inherit">
        {isNil(expeienceInYears)
          ? "N/A"
          : expeienceInYears === 0
          ? "Fresher"
          : expeienceInYears > 1
          ? `${expeienceInYears} years`
          : `${expeienceInYears} year`}
      </Text>
    </Text>
  );
};

const JobCard = ({
  data,
  showSaved = false,
  isSaved,
}: {
  data: JobPaginatedListType["results"][number];
  showSaved?: boolean;
  isSaved?: boolean;
}) => {
  const { isLoggedIn } = useLogin();
  const isMobile = useMediaQuery("(max-width: 425px)");
  const mutateQueryClient = useQueryClient();
  const { makeApiCall } = useApi();
  const { hovered, ref } = useHover();
  const getRedirectUrl = (url: string) => {
    if (!url.includes("?")) {
      return `${url}?jobId=${data.id}`;
    }
    return `${url}&jobId=${data.id}`;
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
          queryKey: [contract.job.filter.path],
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
          queryKey: [contract.job.filter.path],
        });
      },
      showFailureMsg: true,
    });
  };
  const hrefLink = data.isExternalJob
    ? data.externalLink ?? ""
    : `/job/${data.id}/${generateSlugForIdPagePath([
        data.title,
        data.companyName,
        data.cities.map((eachCity) => eachCity.name).join("-"),
        data.modes.join("-"),
        data.industries.map((eachIndustry) => eachIndustry.name).join("-"),
        data.subFunctions
          .map((eachSubFunction) => eachSubFunction.name)
          .join("-"),
      ])}`;
  return (
    <Link
      href={hrefLink}
      target={data.isExternalJob ? "_blank" : "_self"}
      style={{
        textDecoration: "none",
        position: "relative",
        cursor: "pointer",
      }}
    >
      <Stack
        pos="relative"
        ref={ref}
        bg={hovered ? "" : "primaryDarkBlue.9"}
        mx="auto"
        mt={{ base: 40, md: 58 }}
        mb={58}
        pl={{ base: 36, sm: 60, md: 80 }}
        pr={10}
        py={{ base: 36, sm: 40, md: 45 }}
        maw={900}
        style={{
          borderRadius: isMobile ? 10 : 38,
          border: "1px solid",
          borderColor: "var(--mantine-color-primaryGreen-3)",
          boxShadow: hovered
            ? "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)"
            : "0px 0px 0px 0px var(--mantine-color-primaryGreen-3)",
        }}
        gap={0}
        id={`job-id-${data.id}`}
      >
        <Avatar
          pos="absolute"
          w={{ base: 40, md: 50, xl: 60 }}
          h={{ base: 40, md: 50, xl: 60 }}
          left={{ base: -20, xl: -30 }}
          bg="customGray.2"
          style={{
            border: "1px solid",
            borderColor: "var(--mantine-color-primaryGreen-3)",
          }}
          src={data.companyLogo ?? ""}
          alt="company logo"
        />

        <Grid>
          <Grid.Col span={9}>
            <Text
              tt="uppercase"
              c="white"
              fw="400"
              lh="1.17"
              lts={4}
              fz={{ base: 12, sm: 14, xl: 18 }}
              mb={{ base: 10, xl: 12 }}
              maw="80%"
              lineClamp={3}
            >
              {data.companyName}
            </Text>
          </Grid.Col>
          <Grid.Col span={3} p={0} ta="right" pr={20}>
            {showSaved && (
              <CustomButton
                bg="transparent"
                c="white"
                fz={{ base: 14, sm: 16, xl: 18 }}
                leftSection={
                  <>
                    {!isSaved ? (
                      <Image
                        src="/images/bookmarkUnfilled.svg"
                        alt="bookmark"
                        w={{ base: 16, sm: 18, xl: 20 }}
                        h={{ base: 16, sm: 18, xl: 20 }}
                      />
                    ) : (
                      <Image
                        src="/images/bookmark.svg"
                        alt="bookmark"
                        w={{ base: 16, sm: 18, xl: 20 }}
                        h={{ base: 16, sm: 18, xl: 20 }}
                      />
                    )}
                  </>
                }
                onClick={(e) => {
                  e.stopPropagation();
                  e.preventDefault();
                  if (!isLoggedIn) {
                    window.open(
                      `${getApiUrl()}/google?redirectUrl=${encodeURIComponent(
                        getRedirectUrl(window.location.href)
                      )}`,
                      "_self"
                    );

                    return;
                  }
                  if (!isSaved) {
                    saveJob({ jobId: data.id });
                    return;
                  }
                  unsaveJob({ jobId: data.id });
                }}
              />
            )}
          </Grid.Col>
        </Grid>
        <Text
          tt="capitalize"
          c="white"
          fw={700}
          fz={{ base: 18, sm: 20, xl: 28 }}
          lh="1.17"
          mb={{ base: 10, sm: 12, xl: 16 }}
        >
          {data.title}
        </Text>
        <Flex style={{ gap: 10 }} maw={300} mb={28} wrap="wrap">
          {[
            data.industries.map((eachIndustry) => eachIndustry.name).join(", "),
            data.subFunctions
              .map((eachSubFunction) => eachSubFunction.name)
              .join(", "),
            data.modes.join(", "),
          ]
            .filter((eachValue) => eachValue)
            .map((data: string, index: number) => {
              return (
                <Fragment key={index}>
                  <Box
                    onClick={(e) => {
                      e.stopPropagation();
                      e.preventDefault();
                    }}
                  >
                    <CustomToolTip label={data}>
                      <CustomBadge
                        label={data}
                        c={getColor(index)}
                        fz={{ base: 12 }}
                        maw={200}
                        style={{ textOverflow: "ellipsis" }}
                      />
                    </CustomToolTip>
                  </Box>
                </Fragment>
              );
            })}
        </Flex>
        <Group mb={12} gap={isMobile ? 20 : 30} wrap="wrap">
          {!isEmpty(data.cities) && (
            <IconWithLabel
              icon={
                <Image
                  src="/images/location.svg"
                  alt="location"
                  w={{ base: 16, md: 18 }}
                  h={{ base: 16, md: 18 }}
                />
              }
              label={data.cities.map((eachCity) => eachCity.name).join(", ")}
            />
          )}
          {!isEmpty(data.types) && (
            <IconWithLabel
              icon={
                <Image
                  src="/images/clock.svg"
                  alt="time"
                  w={{ base: 16, md: 19 }}
                  h={{ base: 16, md: 19 }}
                />
              }
              label={data.types.join(", ")}
            />
          )}
          {(data.minCtc || data.maxCtc) && (
            <IconWithLabel
              icon={
                <Image
                  src="/images/moneyBag.svg"
                  alt="money"
                  w={{ base: 16, md: 19 }}
                  h={{ base: 16, md: 19 }}
                />
              }
              label={`${[data.minCtc ?? "", data.maxCtc ?? ""]
                .filter((eachValue) => eachValue)
                .join("-")} LPA`}
            />
          )}
        </Group>
        <Flex justify="space-between" wrap="wrap" align="center">
        <ExperienceYearsComponent years={{
              minExp:data.minExp,
              maxExp:data.maxExp}
            }/>
          {data.adminApprovedTime && (
            <CustomBadge
              label={`posted ${getTimeFromNow(data.adminApprovedTime)}`}
              pos="absolute"
              right={{ base: 30, sm: 40 }}
              top={-31}
              px={12}
              py={14}
              style={{
                borderRadius: "10px 10px 0px 0px",
              }}
            />
          )}
        </Flex>
      </Stack>
    </Link>
  );
};
export default JobCard;
