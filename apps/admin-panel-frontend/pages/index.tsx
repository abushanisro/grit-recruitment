import { Stack, Text, Box, Group } from "@mantine/core";
import Head from "next/head";
import PageLayout from "@components/layouts/PageLayout";
import PrimaryButton from "@components/buttons/PrimaryButton";
import Link from "next/link";
import { useRouter } from "next/router";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { useEffect } from "react";
import Navbar from "@components/Navbar";
import CustomSecondaryButton from "@components/buttons/CustomSecondaryButton";

const LandingPage = () => {
  return (
    <>
      <Head>
        <title>Sales Jobverse Admin Panel</title>
        <meta name="description" content="Sales Jobverse Admin Panel" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        navbarComponent={<Navbar />}
        mainComponent={<LandingPageComponent />}
      />
    </>
  );
};

const errorToastId = "errorToastId";

const LandingPageComponent = () => {
  const { showToast } = useCustomToast();
  const router = useRouter();
  const query = router.query;
  const pathname = router.pathname;
  const unauthorizedUser = String(query.ua ?? "");
  const googleLoginError = String(query.googleLoginError ?? "");

  useEffect(() => {
    if (!router.isReady || !unauthorizedUser || unauthorizedUser !== "true") {
      return;
    }
    delete query.ua;
    router.replace({ pathname, query }, undefined, { shallow: true });
    showToast({
      status: ToastStatus.error,
      id: errorToastId,
      message: "Your session has expired. Please log in to access the page.",
    });
  }, [router.isReady, unauthorizedUser]);

  useEffect(() => {
    if (!router.isReady || !googleLoginError || googleLoginError !== "true") {
      return;
    }
    delete query.googleLoginError;
    router.replace({ pathname, query }, undefined, { shallow: true });
    showToast({
      status: ToastStatus.error,
      id: errorToastId,
      message: "Login using Google failed. Please try again after sometime.",
    });
  }, [router.isReady, googleLoginError]);

  return (
    <Box>
      <Stack
        maw={{ base: 300, sm: 600, md: 800, lg: 1100, xl: 1200 }}
        mx="auto"
        miw="100%"
        align="center"
        mb={150}
        mt={{ base: 20, md: 70 }}
      >
        <Text
          fz={{ base: 32, md: 40, xl: 52 }}
          fw="700"
          lh="1.17"
          ta="center"
          py={30}
          mt={{ base: 100, md: 0 }}
          maw={{ md: 600, xl: 761 }}
        >
          Admin Panel for Sales Jobverse
        </Text>
        <Group justify="space-between">
          <Link href="/uploadJobs">
            <PrimaryButton label="Upload Jobs" />
          </Link>
          <Link href="/deleteJobs">
            <CustomSecondaryButton label="Delete Jobs" />
          </Link>
        </Group>
      </Stack>
    </Box>
  );
};

export default LandingPage;
