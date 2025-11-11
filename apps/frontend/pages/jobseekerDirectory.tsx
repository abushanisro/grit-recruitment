import Head from "next/head";
import Navbar from "@components/Navbar";
import { UseFormReturn, useForm } from "react-hook-form";
import PageLayout from "@components/layouts/PageLayout";
import { useQueryState } from "@/hooks/queryState";
import SearchField from "@components/jobs/filters/JobSearchField";

import JobSeekerDirectoryContainer from "@components/jobSeekerDirectory/JobSeekerDirectoryContainer";
import { Box, Button, Group, Text, Title } from "@mantine/core";
import JobSeekeerDirectoryFilters from "@components/jobSeekerDirectory/JobSeekerDirectoryFilters";

import JobSeekerDirectoryFilters from "@components/jobSeekerDirectory/JobSeekerDirectoryFilters";
import { JobseekerDirectoryFilterInterface } from "@/types/jobSeeker";
import { Fragment, useEffect } from "react";
import { useRouter } from "next/router";

import { useUserData } from "@/contexts/UserProvider";
import ConfirmationModal from "@components/jobSeekerDirectory/PaymentConfirmationModal";
import JobSeekerDummyCard from "@components/jobSeekerDirectory/JobSeekerDummyCard";
import { useDisclosure } from "@mantine/hooks";

import CustomModal from "@components/CustomModal";
import PrimaryButton from "@components/buttons/PrimaryButton";
import SubscriptionFilter from "@components/jobSeekerDirectory/SubscriptionFilter";
import { getQueryClient } from "api";
import { getTimeFromNow } from "@/utils/common";
import { IconArrowLeft } from "@tabler/icons-react";
import { BackButton } from "@components/buttons/BackButton";

const JobSeekerDirectoryPage = () => {
  const { userDetails } = useUserData();

  const defaultValueObj = {
    searchText: "",
    pageNumber: 1,
    pageSize: 20,
    locationIds: null,
    maxCTC: null,
    minCTC: null,
    skills: null,
    activeSubscription: null,
    noticePeriod: null,
    ctc: null,
    experience: null,
    portfolio: null,
    lastLogin: null,
    preferredLocations: null,
    customDate: null,
    languageIds: null,
  };

  const [selectedFilters, setSelectedFilters] =
    useQueryState<JobseekerDirectoryFilterInterface>(
      "jobseekerDirectoryPageFilter",
      defaultValueObj
    );

  const hForm = useForm<JobseekerDirectoryFilterInterface>({
    mode: "onChange",
    defaultValues: selectedFilters,
  });

  const { setValue } = hForm;
  const router = useRouter();

  useEffect(() => {
    const setDefaultSubscription = async () => {
      const data =
        await getQueryClient().subscription.suggestionActiveSubscriptions.query();
      if (data.status === 200 && data.body.length > 0) {
        const subscription = {
          label: `${data.body[0].name} will expire ${getTimeFromNow(
            data.body[0].expiryDate
          )}`,
          value: data.body[0].id.toString(),
        };
        setValue("activeSubscription", subscription);
        setSelectedFilters({
          ...selectedFilters,
          activeSubscription: subscription,
        });
      }
    };

    setDefaultSubscription();
  }, []);

  return (
    <>
      <Head>
        <title>Jobseeker Directory</title>
        <meta
          name="description"
          content="Sales Jobverse - Jobseeker Directory"
        />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        pageBg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
        navbarComponent={
          <Navbar>
            <>
              <SearchField name="searchText" hForm={hForm} />
              <BackButton
                label="Go back"
                pathname="/jobseekerDirectoryFilter/"
                query={router.query}
              />
              {userDetails && userDetails.isPurchaseActive && (
                <>
                  <SubscriptionFilter hForm={hForm} />
                  <JobSeekerDirectoryFilters hForm={hForm} />
                </>
              )}
            </>
          </Navbar>
        }
        mainComponent={<JobSeekerDirectoryPageComponent hForm={hForm} />}
      />
    </>
  );
};

const JobSeekerDirectoryPageComponent = ({
  hForm,
}: {
  hForm: UseFormReturn<JobseekerDirectoryFilterInterface>;
}) => {
  const { setValue } = hForm;
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

  const handleButtonClick = () => {
    const queryParams = router.query;

    router.push({
      pathname: "/jobseekerDirectoryFilter/",
      query: queryParams,
    });
  };

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

        <Fragment>
          <JobSeekerDummyCard />
        </Fragment>
      </>
    );
  }
  return (
    <Box>
      <Box display={{ base: "none", sm: "block" }} py={20} w="100%">
        <SubscriptionFilter hForm={hForm} />
        <Text c="primarySkyBlue.6" fw={700} fz={{ base: 30 }}>
          Search results
        </Text>
        <Button
          onClick={() => handleButtonClick()}
          c="primarySkyBlue.6"
          fw={400}
          fz={{ base: 20 }}
          mb={15}
          px={0}
          bg="transparent"
        >
          <IconArrowLeft stroke={2} /> Go back
        </Button>
        <Box maw={1200}>
          <SearchField
            name="searchText"
            hForm={hForm}
            onReset={() => setValue("searchText", "")}
          />
          <Group
            mt={30}
            maw={{ base: "100%", md: "100%", lg: "80%" }}
            style={{
              alignItems: "flex-start",
            }}
          >
            <JobSeekeerDirectoryFilters hForm={hForm} />
          </Group>
        </Box>
      </Box>
      <JobSeekerDirectoryContainer hForm={hForm} />
    </Box>
  );
};

export default JobSeekerDirectoryPage;
