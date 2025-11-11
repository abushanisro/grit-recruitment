import { Box, Space } from "@mantine/core";
import SectionContainer from "@components/SectionContainer";
import ProfileHeader from "@components/profile/jobSeeker/ProfileHeader";
import ProfileBody from "@components/profile/jobSeeker/ProfileBody";
import { ViewJobseekerProfileFromManageApplicationType } from "@/types/employer";
import { ProfileAvatar } from "@/components/profile/jobSeeker/JobSeekerProfileComponent";

const JobSeekerProfile = ({
  data,
}: {
  data: ViewJobseekerProfileFromManageApplicationType;
}) => {
  return (
    <Box
      maw={{ base: 370, xs: 600, sm: 700, md: 800, lg: 1100, xl: 1200 }}
      mt={{ base: 40, sm: 100 }}
      mb={40}
    >
      <SectionContainer
        pl={{ base: 30, xl: 131 }}
        py={{ base: 30, sm: 54 }}
        pr={{ base: 30, sm: 54 }}
        h="auto"
        mih="fit-content"
        style={{ overflow: "visible" }}
      >
        <>
          <ProfileAvatar picture={data.picture ?? ""} />
          <ProfileHeader data={data} />
          <Space h={{ base: 40, sm: 60 }} />
          <ProfileBody data={data} />
        </>
      </SectionContainer>
    </Box>
  );
};
export default JobSeekerProfile;
