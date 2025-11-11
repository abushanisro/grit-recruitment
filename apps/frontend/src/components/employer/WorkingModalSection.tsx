import { Box, Group, SimpleGrid, Stack, Text } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import React from "react";

const WorkingModalSection = ({ procedures }: { procedures: string[] }) => {
  const isTablet = useMediaQuery("(max-width: 992px)");
  const isMobile = useMediaQuery("(max-width: 425px)");

  return (
    <SimpleGrid
      cols={isTablet ? 1 : 3}
      spacing={isTablet ? 20 : 0}
      py={isMobile ? 30 : 50}
    >
      {procedures.map((procedure, index) => {
        return (
          <Stack key={index}>
            <Group
              wrap="nowrap"
              gap={0}
              pos="relative"
              display={isTablet ? "none" : "flex"}
              mb={20}
            >
              <Box
                w="50%"
                h={1}
                bg={index === 0 ? "transparent" : "primaryGreen.3"}
                pos="absolute"
                top="50%"
                left="0%"
              />
              <Box
                w={30}
                h={30}
                pos="absolute"
                left="45%"
                style={{
                  borderRadius: "50%",
                  backgroundColor: "var(--mantine-color-primaryGreen-3)",
                }}
              />
              <Box
                w="50%"
                h={1}
                bg={
                  procedures.length - 1 === index
                    ? "transparent"
                    : "primaryGreen.3"
                }
                pos="absolute"
                top="50%"
                right={0}
              />
            </Group>
            <Group
              wrap="nowrap"
              gap={0}
              justify={isTablet ? "flex-start" : "center"}
              align={isTablet ? "flex-start" : "center"}
            >
              <Box display={isTablet ? "block" : "none"} pt={5}>
                <Box
                  w={15}
                  h={15}
                  style={{
                    borderRadius: "50%",
                    backgroundColor: "var(--mantine-color-primaryGreen-3)",
                  }}
                />
              </Box>
              <Text
                fz={{ base: 20, lg: 25 }}
                fw={600}
                lh="1.25"
                px={20}
                maw={{ base: "100%", lg: 300 }}
                ta={isTablet ? "start" : "center"}
                c="white"
              >
                {procedure}
              </Text>
            </Group>
          </Stack>
        );
      })}
    </SimpleGrid>
  );
};
export default WorkingModalSection;
