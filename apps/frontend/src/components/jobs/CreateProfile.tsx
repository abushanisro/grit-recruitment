import { getApiUrl } from "@/env";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { Box, Image, Text, Stack } from "@mantine/core";
import { useState } from "react";

const CreateProfile = () => {
  const [isHovered, setIsHovered] = useState<boolean>(false);
  return (
    <Box
      mt={58}
      px={32}
      py={28}
      bg="primaryGrey.1"
      pos="relative"
      style={{ borderRadius: "20px" }}
    >
      <Image
        src="/images/lock.svg"
        pos="absolute"
        w="25"
        alt="email-icon"
        top={25}
        right={-10}
      />
      <Stack align="center">
        <Text fw={400} fz={18} lh="1">
          Unlock Your Career Potential Now
        </Text>
        <PrimaryButton
          maw="max-content"
          onMouseEnter={() => setIsHovered(true)}
          onMouseLeave={() => setIsHovered(false)}
          bg={isHovered ? "secondarySkyBlue.5" : "secondarySkyBlue.4"}
          fz={{ base: "13px" }}
          fw={{ base: 700 }}
          label="Create your profile"
          onClick={() => {
            window.open(
              `${getApiUrl()}/google?redirectUrl=${encodeURIComponent(
                window.location.href
              )}`,
              "_self"
            );
          }}
        />
      </Stack>
      <Image
        w={{ base: 80, md: 200 }}
        h={{ base: 70, md: 100 }}
        pos="absolute"
        right={-10}
        top={110}
        src="/images/getNotifiedFasterDesktop.svg"
        alt="info"
      />
    </Box>
  );
};
export default CreateProfile;
