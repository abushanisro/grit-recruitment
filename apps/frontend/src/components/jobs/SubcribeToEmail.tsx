import TextWithArrowComponent from "@components/TextWithArrowComponent";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { Box, Image, Input, Stack } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { useState } from "react";

const SubcribeToEmail = () => {
  const tablet = useMediaQuery("(max-width: 992px)");
  const [email, setEmail] = useState<string>("");
  return (
    <Box
      mt={58}
      px={40}
      py={28}
      bg="secondaryRed.9"
      pos="relative"
      style={{ borderRadius: 20 }}
    >
      <Image
        src="/images/subscribeEmail.svg"
        pos="absolute"
        w={25}
        alt="email-icon"
        top={-10}
        left={40}
      />
      <Stack>
        <Input
          styles={{
            wrapper: {
              border: "0.6px solid",
              borderColor: "white",
              borderRadius: 19,
            },
            input: {
              paddingInline: 14,
              color: "white",
            },
          }}
          variant="unstyled"
          placeholder="Type your email"
          value={email}
          type="email"
          onChange={(e) => {
            setEmail(e.target.value);
          }}
        />
        <PrimaryButton
          label="Subscribe"
          maw="max-content"
          fz={{ base: 13 }}
          fw={{ base: 700 }}
        />
      </Stack>
      <TextWithArrowComponent
        label="For regular job updates"
        left={{ base: "-90px", md: "240px", xl: "240px" }}
        top={{ base: "-60px", md: "-110px", xl: "-110px" }}
        imageProps={{
          style: {
            transform: tablet ? "none" : "rotateZ(85deg)",
          },
        }}
        textProps={{ miw: { base: 100, md: 160 } }}
      />
    </Box>
  );
};
export default SubcribeToEmail;
