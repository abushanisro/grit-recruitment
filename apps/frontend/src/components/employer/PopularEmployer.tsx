import { EmployerBannerDataInterface } from "@/types/employer";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { Center, Flex, Grid, Image, SimpleGrid, Text } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import Link from "next/link";
import React, { Fragment, useState } from "react";
import { EmployerInfoCard } from "@components/employer/EmployerInforCard";

export const freelancerBannerData: EmployerBannerDataInterface[] = [
  {
    location: "Chennai",
    name: "Abhinav Ramesh",
    title:
      "Digital Marketing & Research Senior Associate at Google Operations Centre",
  },
  {
    location: "Mumbai",
    name: "Neha Verma",
    title: "Content Marketing Specialist at Tech Innovations Hub",
  },
  {
    location: "Delhi",
    name: "Arjun Kapoor",
    title: "Social Media Manager at Future Analytics Corporation",
  },
  {
    location: "Bangalore",
    name: "Aishwarya Reddy",
    title: "SEO Analyst at Creative Dynamics Studio",
  },
];
const PopularEmployer = () => {
  const isTablet = useMediaQuery("(max-width: 992px)");
  const isMobile = useMediaQuery("(max-width: 425px)");
  const [isViewAllFreelancersHovered, setIsViewAllFreelancersHovered] =
    useState<boolean>(false);
  return (
    <Grid gutter={5} pb={4} maw={1600} mx="auto">
      <Grid.Col span={isMobile ? 10 : 0}>
        <Flex
          display={isTablet ? "none" : "flex"}
          direction="column"
          h="100%"
          justify="space-between"
          pr={20}
        >
          <Text fz={{ base: 32, md: 40 }} fw={600} lh="1.1" c="white">
            Popular Freelancers
          </Text>
          <Link href="/freelancerDirectory" style={{ textDecoration: "none" }}>
            <PrimaryButton
              label="View all freelancers"
              onMouseEnter={() => setIsViewAllFreelancersHovered(true)}
              onMouseLeave={() => setIsViewAllFreelancersHovered(false)}
              bg={
                isViewAllFreelancersHovered
                  ? "secondaryOrange.4"
                  : "secondaryOrange.3"
              }
              fw={600}
              lh="1.16"
              radius={20}
              rightSection={
                <Image src="/images/rightArrowBlack.svg" alt="" sizes="15px" />
              }
            />
          </Link>
        </Flex>
      </Grid.Col>
      <Grid.Col span="auto">
        <Center display={isTablet ? "flex" : "none"}>
          <Text
            fz={{ base: 32, md: 40 }}
            fw={600}
            lh="1.1"
            c="white"
            mb={isMobile ? 40 : 60}
          >
            Popular Freelancers
          </Text>
        </Center>
        <SimpleGrid cols={isMobile ? 1 : 2} spacing={30}>
          {freelancerBannerData.map((eachData, index) => {
            return (
              <Fragment key={index}>
                <EmployerInfoCard data={eachData} />
              </Fragment>
            );
          })}
        </SimpleGrid>
        <Center display={isTablet ? "flex" : "none"}>
          <Link href="/freelancerDirectory" style={{ textDecoration: "none" }}>
            <PrimaryButton
              label="View all freelancers"
              bg="secondaryOrange.3"
              maw={245}
              fw={600}
              lh="1.16"
              px="lg"
              radius={20}
              mt={60}
              rightSection={
                <Image src="/images/rightArrowBlack.svg" alt="" sizes="15px" />
              }
            />
          </Link>
        </Center>
      </Grid.Col>
    </Grid>
  );
};
export default PopularEmployer;
