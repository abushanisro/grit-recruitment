import { Avatar, Flex, Group, Stack, Text, Image, Box } from "@mantine/core";
import CustomBadge from "@/components/jobs/CustomBadge";
import { Fragment } from "react";
import IconWithLabel from "@/components/jobs/jobcard/IconWithLabel";
import { getColor } from "@/utils/colors";
import {
  AppliedJobPaginatedListType,
  SavedJobPaginatedListType,
} from "@/types/jobs";
import isEmpty from "lodash/isEmpty";
import { useHover } from "@mantine/hooks";
import { useMediaQuery } from "@mantine/hooks";
import { JobApplicationStatus } from "contract/enum";
import { generateSlugForIdPagePath } from "contract/utils";
import Link from "next/link";
import CustomToolTip from "@components/CustomToolTip";
import ExperienceYearsComponent from "@components/ExperienceYearsComponent";

export const getStatusBackgroundColor = (status: string) => {
  if (status === JobApplicationStatus.Shortlisted) {
    return "primarySkyBlue.6";
  } else if (status === JobApplicationStatus.Pending) {
    return "primarySkyBlue.6";
  } else {
    return "secondarySkyBlue.4";
  }
};

const SavedJobsCard = ({
  data,
  showStatus,
  status,
  badgeTimeAgoLabel,
}: {
  data:
    | SavedJobPaginatedListType["results"][number]
    | AppliedJobPaginatedListType["results"][number];
  showStatus?: boolean;
  status?: string;
  badgeTimeAgoLabel: string;
}) => {
  const { hovered, ref } = useHover();
  const isMobile = useMediaQuery("(max-width: 425px)");
  const hrefLink = data.isExternalJob
    ? data.externalLink ?? ""
    : `/job/${data.id}/${generateSlugForIdPagePath([
        data.title,
        data.companyName,
        data.cities.map((city) => city.name).join("-"),
        data.employmentModes.join("-"),
        data.industries.map((eachIndustry) => eachIndustry.name).join("-"),
        data.subFunctions
          .map((eachSubFunction) => eachSubFunction.name)
          .join("-"),
      ])}`;
  return (
    <Link
      href={hrefLink}
      target={data.isExternalJob ? "_blank" : "_self"}
      style={{ textDecoration: "none", position: "relative" }}
    >
      <Stack
        pos="relative"
        ref={ref}
        bg={
          hovered
            ? ""
            : "linear-gradient(276deg, #1A264D 0.51%, #031D21 96.52%)"
        }
        ml={{ base: 10, md: 0 }}
        mt={{ base: 40, md: 58 }}
        mb={58}
        pl={{ base: 36, sm: 60, md: 80 }}
        pr={{ base: 10, sm: 20, md: 30 }}
        py={{ base: 36, sm: 40, md: 45 }}
        style={{
          borderRadius: isMobile ? 10 : 38,
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryGreen-1)",
          boxShadow: hovered
            ? "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)"
            : "0px 0px 0px 0px var(--mantine-color-primaryGreen-3)",
        }}
        gap={0}
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

        <CustomBadge
          label={badgeTimeAgoLabel}
          pos="absolute"
          right={{ base: 20, sm: 40 }}
          top={-31}
          px={12}
          py={14}
          style={{
            borderRadius: "10px 10px 0px 0px",
          }}
        />

        <Text
          tt="uppercase"
          c="white"
          fw="400"
          lh="1.17"
          lts={4}
          fz={{ base: 12, sm: 14, xl: 18 }}
          mb={{ base: 10, xl: 12 }}
        >
          {data.companyName}
        </Text>
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
        <Flex
          style={{ gap: 10 }}
          maw={{ base: 300, lg: "100%" }}
          mb={28}
          wrap={"wrap"}
        >
          {[
            data.industries.map((eachIndustry) => eachIndustry.name).join(", "),
            data.subFunctions
              .map((eachSubFunction) => eachSubFunction.name)
              .join(", "),
            data.employmentModes.join(", "),
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
                        maw={{ base: 200 }}
                        style={{ textOverflow: "ellipsis" }}
                      />
                    </CustomToolTip>
                  </Box>
                </Fragment>
              );
            })}
        </Flex>
        <Group mb={12} gap={30} wrap="wrap">
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
          {!isEmpty(data.employmentTypes) && (
            <IconWithLabel
              icon={
                <Image
                  src="/images/clock.svg"
                  alt="time"
                  w={{ base: 16, md: 19 }}
                  h={{ base: 16, md: 19 }}
                />
              }
              label={data.employmentTypes.join(", ")}
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
          {showStatus && status && (
            <CustomBadge
              label={status}
              bg={getStatusBackgroundColor(status)}
              c="black"
              mr={10}
              py={{ base: 6, sm: 12 }}
              px={{ base: 4, sm: 8 }}
              fz={{ base: 12, sm: 16 }}
            />
          )}
        </Flex>
      </Stack>
    </Link>
  );
};
export default SavedJobsCard;
