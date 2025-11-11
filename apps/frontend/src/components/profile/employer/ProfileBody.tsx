import CustomBadge from "@components/jobs/CustomBadge";
import CustomContent from "@components/jobs/jobId/body/CustomContent";
import SubHeading from "@components/jobs/jobId/body/SubHeading";
import { BadgeProps, Divider, Flex, FlexProps, Text } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { Fragment } from "react";
import isEmpty from "lodash/isEmpty";
import { EmployerUserDataType } from "@/types/employer";
import isNil from "lodash/isNil";
import { getCompanySizeLabel } from "@/components/profile/employer/EmployerProfileForm";

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
      <Text fw={400} fz={{ base: 16, xl: 22 }} lh="1.17" c="secondaryGreen.1">
        {sideHeading}
      </Text>
      <Divider orientation="vertical" h={30} />
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
const IndustriesList = ({ industries }: { industries: string[] }) => {
  return (
    <>
      {industries.map((industry: string, index: number) => {
        return (
          <Fragment key={index}>
            <ProfileCustomBadge label={industry} />
          </Fragment>
        );
      })}
    </>
  );
};

const ProfileBody = ({ data }: { data: EmployerUserDataType }) => {
  const tablet = useMediaQuery("(max-width: 992px)");
  return (
    <>
      {data.aboutCompany && (
        <>
          <SubHeading label="About Company" c="secondaryGreen.1" />
          <Divider
            style={{ borderColor: "var(--mantine-color-secondaryGreen-1)" }}
            my={18}
            maw="50%"
          />
          <CustomContent label={data.aboutCompany} maw="100%" ta="justify" mb={40} />
        </>
      )}
      {!isEmpty(data.company.industries) && (
        <SideHeadingWithContent sideHeading="Industries">
          <IndustriesList
            industries={data.company.industries.map(
              (eachIndustry) => eachIndustry.name
            )}
          />
        </SideHeadingWithContent>
      )}
      {!isNil(data.companySize) && (
        <Flex wrap="wrap" direction="row" gap={tablet ? 16 : 30} my={40}>
          <SideHeadingWithContent sideHeading="Company Size">
            <Text
              fw={400}
              fz={{ base: 16, xl: 22 }}
              lh="1.17"
              c="white"
              tt="capitalize"
            >
              {getCompanySizeLabel(data.companySize)}
            </Text>
          </SideHeadingWithContent>
        </Flex>
      )}
    </>
  );
};

export default ProfileBody;
