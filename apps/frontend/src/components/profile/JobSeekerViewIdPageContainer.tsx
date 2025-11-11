import { Avatar, Box, Divider } from "@mantine/core";
import SectionContainer from "@components/SectionContainer";
import { getQueryClient } from "api";
import { contract } from "contract";
import { useMediaQuery } from "@mantine/hooks";
import ProfileLoader from "@components/loaders/ProfileLoader";
import ErrorMessage from "@components/ErrorMessage";
import ProfileHeader from "@/components/profile/jobSeeker/ProfileHeader";
import ProfileBody from "@/components/profile/jobSeeker/ProfileBody";
import YellowText from "@/components/YellowText";

import { IconPointFilled } from "@tabler/icons-react";

import { JobSeekerUserDataType } from "@/types/jobSeeker";

export const PhoneNumberAndEmailSection = ({
  email,
  phoneNumber,
}: {
  email: string;
  phoneNumber: string;
}) => {
  return (
    <>
      <Divider orientation="vertical" h={30} color="secondaryYellow.3" />
      <YellowText label={email} />
      <IconPointFilled
        style={{
          color: "var(--mantine-color-secondaryOrange-3)",
        }}
      />
      <YellowText label={phoneNumber} />
    </>
  );
};

export const ProfileAvatar = ({ picture }: { picture: string }) => {
  return (
    <Avatar
      w={{ base: 60, sm: 100, md: 140, xl: 180 }}
      h={{ base: 60, sm: 100, md: 140, xl: 180 }}
      bg="customGray.2"
      style={{
        border: "1px solid",
        borderColor: "var(--mantine-color-secondaryGreen-1)",
      }}
      src={picture}
      alt="profile pic"
    />
  );
};

export const JobSeekerProfile = ({
  data,
}: {
  data: {
    body: JobSeekerUserDataType;
  };
}) => {
  const isDesktop = useMediaQuery("(min-width: 1440px)");

  return (
    <Box
      maw={{ base: 370, xs: 600, sm: 700, md: 800, lg: 1100, xl: 1200 }}
      mt={{ base: 40, sm: 100 }}
      mb={40}
    >
      <SectionContainer
        pl={{ base: 30, sm: 50, xl: 131 }}
        py={{ base: 30, sm: 54, xl: 100 }}
        pr={{ base: 30, sm: 54 }}
        h="auto"
        mih="fit-content"
        style={{ overflow: "visible" }}
      >
        <>
          <Box
            w={{ base: 60, sm: 100, md: 140, xl: 180 }}
            h={{ base: 60, sm: 100, md: 140, xl: 180 }}
            pos="absolute"
            left={{
              base: "20px",
              sm: isDesktop ? "120px" : "40px",
            }}
            top={{
              base: "-30px",
              sm: "-50px",
              md: "-70px",
              lg: "-85px",
              xl: "-85px",
            }}
            style={{ borderRadius: "100%", overflow: "hidden" }}
          >
            <ProfileAvatar picture={data.body.picture ?? ""} />
          </Box>
          <ProfileHeader data={data.body} />

          <ProfileBody data={data.body} />
        </>
      </SectionContainer>
    </Box>
  );
};

const JobSeekerViewIdPageContainer = ({ id }: { id: string }) => {
  const { data, isLoading, error } =
    getQueryClient().subscription.getJobSeekerProfile.useQuery(
      [contract.subscription.getJobSeekerProfile.path, id],
      { query: { employerProfileViewId: id } },
      {
        retry: (failureCount, error) => {
          return error.status >= 500 && error.status < 600 && failureCount < 3;
        },
      }
    );

  if (isLoading) {
    return <ProfileLoader />;
  }
  if (error) {
    return <ErrorMessage error={error} />;
  }
  return <JobSeekerProfile data={data} />;
};
export default JobSeekerViewIdPageContainer;
