import { Stack, Text, Box, Flex, Image } from "@mantine/core";
import Head from "next/head";

import PageLayout from "@components/layouts/PageLayout";
import Navbar from "@components/Navbar";
import BubbleLinkButton from "@/components/buttons/BubbleLinkButton";
import Footer from "@components/Footer";
import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { useMediaQuery } from "@mantine/hooks";
import { UserRole } from "contract/enum";
import { useUserData } from "@/contexts/UserProvider";
const errorToastId = "errorToastId";
type RegularUserRoles = Exclude<UserRole, "admin">;

const LandingPage = () => {
  return (
    <>
      <Head>
        <title>Sales Jobverse</title>
        <meta name="description" content="Sales Jobverse" />
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

const landingPageRoleMap: { [key: string]: string } = {
  [UserRole.jobSeeker]: "/jobs",
  [UserRole.employer]: "/employer",
};

const LandingPageComponent = () => {
  const { showToast } = useCustomToast();
  const isSmallDesktop = useMediaQuery("(min-width: 1024px)");
  const [isJobSeekerHovered, setIsJobSeekerHovered] = useState<boolean>(false);
  const [isEmployerHovered, setIsEmployerHovered] = useState<boolean>(false);

  const router = useRouter();
  const { userDetails } = useUserData();
  const authenticationToastId = "authenticationToastId";
  const query = router.query;
  const pathname = router.pathname;
  const unauthorizedUser = String(query.ua ?? "");
  const googleLoginError = String(query.googleLoginError ?? "");

  const handleRoleNavigation = (role: RegularUserRoles) => {
    if (!userDetails || role === userDetails.role) {
      router.push(landingPageRoleMap[role]);
    } else {
      showToast({
        status: ToastStatus.error,
        id: authenticationToastId,
        message: `Signup/login to connect as ${role.toLowerCase()}.`,
      });
    }
  };

  useEffect(() => {
    if (!userDetails) {
      Object.values(landingPageRoleMap).forEach((path) =>
        router.prefetch(path)
      );
      return;
    }
    router.prefetch(landingPageRoleMap[userDetails.role]);
  }, [userDetails]);

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
          maw={{ md: 600, xl: 761 }}
        >
          Discover sales jobs <br /> and talent
        </Text>
        <Flex
          direction={{ base: "column", md: "row" }}
          justify="space-between"
          align="center"
          gap={{ base: 80, xl: 160 }}
          mt={{ base: 100, md: 120 }}
          wrap={"wrap"}
        >
          <Box pos="relative">
            {isSmallDesktop ? (
              <Image
                w={{ base: 80, md: 130 }}
                h={{ base: 70, md: 120 }}
                pos="absolute"
                left={{ base: "-90px", md: "-120px" }}
                top={{ base: "-60px", md: "40px" }}
                // NOTE : To stop flicker of image in desktop when isSmallDesktop is undefined on page initial load
                src={"/images/jobseekerLogo.svg"}
                alt="info"
              />
            ) : (
              <Image
                w={{ base: 84, md: 130 }}
                h={{ base: 67, md: 120 }}
                pos="absolute"
                left={{ base: "-80px", md: "-120px" }}
                top={{ base: "-50px", md: "40px" }}
                // NOTE : To stop flicker of image in desktop when isSmallDesktop is undefined on page initial load
                src={"/images/mobileLookingForJob.svg"}
                alt="info"
              />
            )}
            <BubbleLinkButton
              onMouseEnter={() => setIsJobSeekerHovered(true)}
              onMouseLeave={() => setIsJobSeekerHovered(false)}
              handleClick={() => handleRoleNavigation(UserRole.jobSeeker)}
              label="Jobseeker"
              bg={isJobSeekerHovered ? "primarySkyBlue.7" : "primarySkyBlue.6"}
              c="black"
            />
          </Box>

          <Box pos="relative">
            {isSmallDesktop ? (
              <Image
                w={{ base: 80, md: 130 }}
                h={{ base: 60, md: 120 }}
                pos="absolute"
                left={{ base: "-90px", md: "160px", xl: "180px" }}
                top={{ base: "-60px", md: "40px" }}
                // NOTE : To stop flicker of image in desktop when isSmallDesktop is undefined on page initial load
                src={"/images/employerLogo.svg"}
                alt="info"
              />
            ) : (
              <Image
                w={{ base: 84, md: 130 }}
                h={{ base: 67, md: 120 }}
                pos="absolute"
                left={{ base: "156px", md: "156px" }}
                top={{ base: "40px", md: "40px" }}
                // NOTE : To stop flicker of image in desktop when isSmallDesktop is undefined on page initial load
                src={"/images/mobileEmployerLogo.svg"}
                alt="info"
              />
            )}

            <BubbleLinkButton
              label="Employer"
              onMouseEnter={() => setIsEmployerHovered(true)}
              onMouseLeave={() => setIsEmployerHovered(false)}
              handleClick={() => handleRoleNavigation(UserRole.employer)}
              bg={isEmployerHovered ? "primaryBlue.8" : "primaryBlue.9"}
              c="white"
            />
          </Box>
        </Flex>
      </Stack>
      <Footer />
    </Box>
  );
};

export default LandingPage;
