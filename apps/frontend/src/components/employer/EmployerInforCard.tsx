import { EmployerBannerDataInterface } from "@/types/employer";
import { Avatar, Box, Group, Image, Stack, Text } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import React from "react";
import EmployerHomePageCard from "@components/employer/EmployerHomePageCard";

export const EmployerInfoCard = ({
  data,
}: {
  data: EmployerBannerDataInterface;
}) => {
  const isTablet = useMediaQuery("(max-width: 992px)");
  return (
    <EmployerHomePageCard
      style={{
        borderColor: "var(--mantine-color-secondaryGreen-1)",
        boxShadow: `0px 4px 4px 0px var(--mantine-color-primaryGreen-4)`,
        padding: 20,
        height: "40vh !important",
        borderRadius: "24px",
      }}
      h="100%"
    >
      <Group>
        <Box
          style={{
            border: "1px solid var(--mantine-color-secondaryGreen-1)",
            borderRadius: "50%",
          }}
        >
          <Avatar size={isTablet ? 60 : 80} alt="profile pic" />
        </Box>
        <Stack gap={10}>
          <Group wrap="nowrap" gap={10}>
            <Image src="/images/coloredLocation.svg" alt="" sizes="20px" />
            <Text fz={{ base: 14, md: 18 }} lh="1.16" c="white">
              {data.location}
            </Text>
          </Group>
          <Text fz={{ base: 16, md: 21 }} lh="1.14" c="white" fw={700}>
            {data.name}
          </Text>
        </Stack>
      </Group>
      <Text fz={{ base: 14, md: 18 }} lh="1.16" fw={600} c="#F1CD7C" mt={35}>
        {data.title}
      </Text>
    </EmployerHomePageCard>
  );
};
