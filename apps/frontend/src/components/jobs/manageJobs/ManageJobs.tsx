import { Box, Text } from "@mantine/core";
import SectionContainer from "@components/SectionContainer";
import JobSeekerJobsList from "@/components/jobs/manageJobs/JobSeekerJobsList";

const ManageJobs = () => {
  return (
    <Box my={{ base: 20, md: 32 }}>
      <Text
        fz={{ base: 28, sm: 32 }}
        fw={600}
        style={{ letterSpacing: "2px" }}
        mb={20}
      >
        Jobs
      </Text>
      <SectionContainer mih="60vh" h="100%">
        <JobSeekerJobsList />
      </SectionContainer>
    </Box>
  );
};

export default ManageJobs;
