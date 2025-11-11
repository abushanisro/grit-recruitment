import PrimaryButton from "@components/buttons/PrimaryButton";
import {
  Box,
  Center,
  Divider,
  Image,
  SimpleGrid,
  Stack,
  Text,
} from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import React, { useState } from "react";
import Link from "next/link";
import { getApiUrl } from "@/env";
import { useLogin } from "@/contexts/LoginProvider";
import WorkingModalSection from "@components/employer/WorkingModalSection";
import FreelancerHomePageCard from "@components/employer/EmployerHomePageCard";
import ColoredBox from "@components/employer/ColoredBox";
import FAQ from "@components/employer/Faq";
import { FaqQuestionAnswerInterface } from "@/types/employer";
import { useRouter } from "next/router";

const faqQuestionAndAnswers: FaqQuestionAnswerInterface[] = [
  {
    id: "FAQ1",
    question:
      "What is Sales Jobverse and how can employers benefit from using it?",
    answer:
      "Sales Jobverse is a dedicated platform exclusively for sales jobs. Employers can leverage this platform to post full-time sales positions for jobseekers within the sales domain.",
  },
  {
    id: "FAQ2",
    question: "  How do I post a job on Sales Jobverse for jobseekers?",
    answer: `Posting a job for jobseekers on Sales Jobverse is a straightforward process. Log in to your employer account, locate the "Post a Job" button, input the job details, and submit the listing. Once approved, your job will be visible to our community of sales professionals seeking various roles.`,
  },
  {
    id: "FAQ3",
    question: "What are the charges for using your platform?",
    answer:
      "Sales Jobverse is currently free for employers. Sign up, & post job listing without any charges.",
  },
  {
    id: "FAQ4",
    question: " Can I manage applications on Sales Jobverse?",
    answer:
      "Yes, employers can efficiently manage job applications on our platform, streamlining the hiring process.",
  },
  {
    id: "FAQ5",
    question:
      "How can Sales Jobverse help me find the right candidates quickly?",
    answer:
      "Sales Jobverse sends your job post to relevant candidates in our database, ensuring your job reaches individuals whose interests align with your sales position.",
  },
  {
    id: "FAQ6",
    question: "How does communication take place?",
    answer:
      "While initial discovery happens on our platform, you are free to take the discussion off the platform to close your hiring requirements with jobseekers.",
  },
];

const EmployerHomePage = () => {
  const isTablet = useMediaQuery("(max-width: 992px)");
  const isMobile = useMediaQuery("(max-width: 425px)");
  const [isPostJobHovered, setIsPostJobHovered] = useState<boolean>(false);
  const { isLoggedIn } = useLogin();
  const router = useRouter();

  const handleOpen = () => {
    if (!isLoggedIn) {
      window.open(
        `${getApiUrl()}/google?redirectUrl=${encodeURIComponent(
          window.location.href
        )}`,
        "_self"
      );

      return;
    } else {
      router.push("/postJob");
    }
  };

  return (
    <Stack align="center" gap={30}>
      <Box pb={50} maw={1600} mx="auto" mt={100}>
        <Box>
          <Text fz={{ base: 36, md: 70 }} fw={700} lh="1.07" ta="center">
            Recruit sales talent
          </Text>
          <Text fz={{ base: 20, md: 45 }} fw={400} lh="1.66" ta="center">
            for full-time
          </Text>
        </Box>
        <Center pos="relative" mt={46}>
          <PrimaryButton
            fw={600}
            label="Post a Job"
            c="black"
            maw={245}
            onMouseEnter={() => setIsPostJobHovered(true)}
            onMouseLeave={() => setIsPostJobHovered(false)}
            bg={isPostJobHovered ? "secondarySkyBlue.5" : "secondarySkyBlue.4"}
            px="lg"
            radius={20}
            onClick={handleOpen}
            rightSection={
              <Image src="/images/rightArrowBlack.svg" alt="" sizes="15px" />
            }
            style={{
              zIndex: 5,
            }}
          />
        </Center>
      </Box>
      <Box w="100%" maw={1600} mx="auto">
        <FreelancerHomePageCard
          style={{
            borderColor: "var(--mantine-color-primaryGreen-3)",
            boxShadow: `0px 4px 4px 0px var(--mantine-color-primaryGreen-3)`,
            marginTop: isMobile ? 0 : 40,
          }}
          h="100%"
        >
          <SimpleGrid cols={isTablet ? 1 : 2} h="100%" spacing={40}>
            <Stack gap="xs" justify="center">
              <Text
                fz={{ base: 32, md: 40 }}
                fw={700}
                lh="1.37"
                ta={{ base: "center", md: "left" }}
                c="white"
              >
                Discover Talent
              </Text>
              <Text
                fz={{ base: 20, md: 24 }}
                fw={500}
                lh="2.29"
                ta={{ base: "center", md: "left" }}
                c="white"
              >
                Access video introductions of sales talent
              </Text>

              <Link
                href={`/manageJobs`}
                style={{
                  width: "max-content",
                  marginInline: isMobile ? "auto" : isTablet ? "auto" : 0,
                }}
              >
                <PrimaryButton
                  fw={600}
                  label="Manage Jobs"
                  c="black"
                  maw="max-content"
                  mt={{ lg: 56 }}
                  px="lg"
                  radius={20}
                  rightSection={
                    <Image
                      src="/images/rightArrowBlack.svg"
                      alt=""
                      sizes="15px"
                    />
                  }
                  style={{
                    zIndex: 5,
                  }}
                />
              </Link>
            </Stack>
            <SimpleGrid
              cols={isMobile ? 1 : 2}
              spacing={isTablet ? "xs" : "md"}
            >
              <ColoredBox
                style={{
                  alignSelf: "flex-start",
                  justifySelf: isMobile ? "center" : "flex-start",
                  backgroundColor: "var(--mantine-color-secondaryGreen-1)",
                  maxWidth: isMobile ? "100%" : 245,
                }}
              >
                <Text fz={{ base: 16, md: 21 }} fw={700} lh="1.47" c="black">
                  Scroll through our available directory of jobseekers
                </Text>
              </ColoredBox>
              <ColoredBox
                style={{
                  alignSelf: "flex-end",
                  justifySelf: isMobile ? "center" : "flex-end",
                  backgroundColor: "var(--mantine-color-primaryGreen-3)",
                  maxWidth: isMobile ? "100%" : 245,
                }}
              >
                <Text fz={{ base: 16, md: 21 }} fw={700} lh="1.47" c="black">
                  Video interviews make the hiring process smoother!
                </Text>
              </ColoredBox>
            </SimpleGrid>
          </SimpleGrid>
          <Image
            alt="plant"
            src="/images/ionPeople.svg"
            maw={isMobile ? 73 : 73}
            pos="absolute"
            top={isMobile ? -45 : isTablet ? -50 : -50}
            right={50}
          />
        </FreelancerHomePageCard>
      </Box>

      <Box w="100%" py={isMobile ? 50 : 100}>
        <Divider
          label={
            <Text
              fz={{ base: 32, md: 40 }}
              fw={700}
              lh="1.19"
              c="white"
              lts={2}
              px={isMobile ? 0 : 30}
            >
              HOW IT WORKS
            </Text>
          }
          color="secondaryGreen.1"
          h={2}
          w="100%"
          labelPosition="center"
        />
        <Box w="100%" maw={1600} mx="auto" py={{ base: 40, sm: 100 }}>
          <WorkingModalSection
            procedures={[
              "Get started by creating your profile",
              "Create hiring calls for jobseekers",
              "Get access to our jobseeker directory",
            ]}
          />
        </Box>
        <Divider color="secondaryGreen.1" h={2} w="100%" />
      </Box>
      <FAQ faqQuestionAndAnswers={faqQuestionAndAnswers} />
    </Stack>
  );
};

export default EmployerHomePage;
