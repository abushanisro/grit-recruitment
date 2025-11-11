import React from "react";
import { Stack, Text } from "@mantine/core";
const postPaidModalHeader = () => {
  return (
    <Stack gap={4}>
      <Text fz={{ base: 20, md: 28 }} fw={700} c="white">
        Promote your job post
      </Text>
      <Text fz={{ base: 12, sm: 16 }} c="white">
        Promote your job post and get more visibility to your job listing
      </Text>
    </Stack>
  );
};

export default postPaidModalHeader;
