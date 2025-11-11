import { JobSeekerUserDataType } from "@/types/jobSeeker";
import CustomBadge from "@components/jobs/CustomBadge";
import CustomContent from "@components/jobs/jobId/body/CustomContent";
import SubHeading from "@components/jobs/jobId/body/SubHeading";
import { BadgeProps, Divider, Flex, FlexProps, Text } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { Fragment } from "react";
import isEmpty from "lodash/isEmpty";
import { getNoticePeriodLabel } from "@/utils/common";

const SideHeadingWithContent = ({
  sideHeading,
  children,
  ...props
}: {
  sideHeading: string;
  children: React.ReactNode;
} & FlexProps) => {
  const tablet = useMediaQuery("(max-width: 992px)");
  return (
    <Flex wrap="wrap" gap={tablet ? 14 : 20} {...props} align="center">
      <Text fw={700} fz={{ base: 16, xl: 22 }} lh="1.17" c="secondaryGreen.1">
        {sideHeading}
      </Text>
      <Divider orientation="vertical" color="secondaryGreen.1" />
      {children}
    </Flex>
  );
};

export const ProfileCustomBadge = ({
  label,
  ...props
}: { label: string } & BadgeProps) => {
  return (
    <CustomBadge
      label={label}
      bg="primaryGrey.1"
      c="white"
      px={16}
      py={16}
      fz={{ base: 16, md: 18 }}
      tt="capitalize"
      {...props}
    />
  );
};
const SkillsList = ({ skills }: { skills: string[] }) => {
  return (
    <>
      {skills.map((skill: string, index: number) => {
        return (
          <Fragment key={index}>
            <ProfileCustomBadge label={skill} />
          </Fragment>
        );
      })}
    </>
  );
};

const ProfileBody = ({ data }: { data: JobSeekerUserDataType }) => {
  const tablet = useMediaQuery("(max-width: 992px)");
  const noticePeriodLabel = getNoticePeriodLabel(data.noticePeriod);
  return (
    <>
      {data.profileSummary && (
        <>
          <SubHeading label="About me" c="secondaryGreen-1" />
          <Divider
            style={{ borderColor: "var(--mantine-color-secondaryGreen-1)" }}
            my={18}
            maw="50%"
          />
          <CustomContent label={data.profileSummary} maw="100%" mb={40} />
        </>
      )}
      {!isEmpty(data.skills) && (
        <SideHeadingWithContent sideHeading="Skills">
          <SkillsList skills={data.skills.map((eachSkill) => eachSkill.name)} />
        </SideHeadingWithContent>
      )}
      {data.expectedSalaryInLpa && (
        <SideHeadingWithContent sideHeading="Expected Salary" my={40}>
          <Text fw={400} fz={{ base: 16, xl: 22 }} lh="1.17" c="white">
            {`${data.expectedSalaryInLpa} LPA`}
          </Text>
        </SideHeadingWithContent>
      )}
      <Flex wrap="wrap" direction="row" gap={tablet ? 40 : 30} my={40}>
        <SideHeadingWithContent sideHeading="Min. Experience">
          <Text fw={400} fz={{ base: 16, xl: 22 }} lh="1.17" c="white">
            {data.experienceInYear === 0
              ? "Fresher"
              : data.experienceInYear > 1
              ? `${data.experienceInYear} years`
              : `${data.experienceInYear} year`}
          </Text>
        </SideHeadingWithContent>
        <SideHeadingWithContent sideHeading="Notice Period">
          <Text
            fw={400}
            fz={{ base: 16, xl: 22 }}
            lh="1.17"
            c="white"
            tt="capitalize"
          >
            {!noticePeriodLabel ? "N/A" : noticePeriodLabel}
          </Text>
        </SideHeadingWithContent>
      </Flex>
      {!isEmpty(data.preferredLocations) && (
        <SideHeadingWithContent sideHeading="Preferred Locations" my={40}>
          {data.preferredLocations.map((preferredCity, index: number) => {
            return (
              <Fragment key={index}>
                <ProfileCustomBadge label={preferredCity.name} c="white" />
              </Fragment>
            );
          })}
        </SideHeadingWithContent>
      )}
      {!isEmpty(data.languages) && (
        <SideHeadingWithContent sideHeading="Languages" my={40}>
          {data.languages.map((language, index: number) => {
            return (
              <Fragment key={index}>
                <ProfileCustomBadge label={language.name} c="white" />
              </Fragment>
            );
          })}
        </SideHeadingWithContent>
      )}
    </>
  );
};

export default ProfileBody;
