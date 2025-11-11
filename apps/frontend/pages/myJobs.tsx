import Head from "next/head";
import { useQueryState } from "@/hooks/queryState";
import PageLayout from "@components/layouts/PageLayout";
import Navbar from "@components/Navbar";
import { Box, Group, Text } from "@mantine/core";
import isNil from "lodash/isNil";
import SavedJobsList from "@components/jobs/SavedJobsList";
import AppliedJobsList from "@components/jobs/AppliedJobsList";
import { useMediaQuery } from "@mantine/hooks";
import SectionContainer from "@components/SectionContainer";
import { ActiveTableTabButton } from "@components/buttons/ActiveTableTabButton";

const MyJobsPage = () => {
  return (
    <>
      <Head>
        <title>My Jobs | Sales Jobverse</title>
        <meta name="description" content="Sales Jobverse - My Jobs" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        navbarComponent={<Navbar />}
        mainComponent={<MyJobsPageComponent />}
      />
    </>
  );
};
export enum TabsEnum {
  appliedJobs = "appliedJobs",
  savedJobs = "savedJobs",
}
const MyJobsPageComponent = () => {
  const isTablet = useMediaQuery("(max-width: 768px)");
  const [activeTab, setActiveTab] = useQueryState<string | null>(
    "activeTab",
    TabsEnum.appliedJobs
  );

  if (isNil(activeTab) || !activeTab) {
    return <></>;
  }
  return (
    <Box mt={{ base: 0, md: 30 }}>
      <Text fz={{ base: 28, sm: 32 }} fw={600} style={{ letterSpacing: "2px" }}>
        My Jobs
      </Text>
      <Group
        justify="flex-start"
        wrap="nowrap"
        pl={{ base: 0, sm: 40, md: 80 }}
        mt={{ base: 12, md: 24 }}
        py={isTablet ? 16 : 0}
        style={{
          overflowX: "scroll",
        }}
      >
        {Object.values(TabsEnum).map((applicationStatus, index) => {
          const isActive = activeTab === applicationStatus;
          return (
            <ActiveTableTabButton
              key={index}
              isActive={isActive}
              label={
                applicationStatus === TabsEnum.appliedJobs
                  ? "Applied Jobs"
                  : "Saved Jobs"
              }
              onClick={() => {
                setActiveTab(applicationStatus);
              }}
            />
          );
        })}
      </Group>

      <SectionContainer>
        {activeTab === TabsEnum.savedJobs ? (
          <SavedJobsList />
        ) : (
          <AppliedJobsList />
        )}
      </SectionContainer>
    </Box>
  );
};

export default MyJobsPage;
