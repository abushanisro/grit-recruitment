import Head from "next/head";
import JobSeekeerDirectoryFilterContainer from "@components/jobSeekerDirectory/JobSeekerDirectoryFilterContainer";
import PageLayout from "@components/layouts/PageLayout";
import Navbar from "@components/Navbar";
import { Box, Text, Title } from "@mantine/core";
import { useUserData } from "@/contexts/UserProvider";
import { useRouter } from "next/router";
import { useDisclosure } from "@mantine/hooks";
import { useEffect } from "react";
import ConfirmationModal from "@components/jobSeekerDirectory/PaymentConfirmationModal";
import PrimaryButton from "@components/buttons/PrimaryButton";
import JobSeekerDummyCard from "@components/jobSeekerDirectory/JobSeekerDummyCard";
import CustomModal from "@components/CustomModal";

const JobSeekeerDirectoryFilterPage = () => {
  return (
    <>
      <Head>
        <title>Jobseeker Directory Filter</title>
        <meta
          name="description"
          content="Sales Jobverse - Jobseeker Directory Filter"
        />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        pageBg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
        navbarComponent={<Navbar></Navbar>}
        mainComponent={<JobSeekeerDirectoryFilterPageComponent />}
      />
    </>
  );
};

const JobSeekeerDirectoryFilterPageComponent = () => {
  const { userDetails } = useUserData();

  const router = useRouter();
  const [opened, { open, close }] = useDisclosure(false);
  const [
    openedUnverified,
    { open: onUnverifiedOpen, close: onUnverifiedClose },
  ] = useDisclosure(false);

  useEffect(() => {
    if (userDetails && !userDetails.isVerified) {
      onUnverifiedOpen();
    }
  }, []);
  useEffect(() => {
    if (opened || (userDetails && userDetails.isPurchaseActive)) {
      return;
    }
    open();
  }, []);

  const handleClick = () => {
    router.push("/subscription");
    close();
  };
  if (userDetails && !userDetails.isVerified) {
    return (
      <>
        <CustomModal opened={openedUnverified} onClose={onUnverifiedClose}>
          <Title order={3} c="white" mb={40}>
            Sorry you do not have the access to view JobseekerDirectory <br />
            until you are verified by admin.
          </Title>
          <Box ta="right">
            <PrimaryButton
              label={"Go to Home Page"}
              fz={{ base: 16, sm: 20 }}
              fw="600"
              px={20}
              py={12}
              w={{ base: "100%", xs: "max-content" }}
              ml={{ base: 0, xs: 20 }}
              mt={{ base: 10, xs: 0 }}
              h="max-content"
              onClick={() => router.push("/employer")}
            />
          </Box>
        </CustomModal>
      </>
    );
  } else if ((userDetails && !userDetails.isPurchaseActive) || !userDetails) {
    return (
      <>
        <ConfirmationModal
          opened={opened}
          onClose={close}
          modalHeader={`Please subscribe to our plans to view this page`}
          onSuccessFn={handleClick}
          successButtonLabel="View Plans"
          secondaryButtonLabel="Cancel"
        />

        <>
          <JobSeekerDummyCard />
        </>
      </>
    );
  }
  return (
    <Box>
      <Text c="secondaryGreen.1" fw={700} fz={{ base: 30 }} pt={30} pb={30}>
        Filter job seekers
      </Text>
      <JobSeekeerDirectoryFilterContainer />
    </Box>
  );
};

export default JobSeekeerDirectoryFilterPage;
