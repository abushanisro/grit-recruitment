import { Text, Divider, Box, Flex} from "@mantine/core";
import CustomLink from "@components/buttons/CustomLink";
import InstagramIconButton from "@components/socialMediaButtons/InstagramIconButton";
import { instagramLink } from "@/env";

import EmailIconButton from "@components/socialMediaButtons/EmailIconButton";
const Footer = () => {
  return (
    <Box py={40} bg="inherit" c="white">
      <Divider mb={48} bg="customGray.5" />
      <Flex
        justify="space-between"
        align={{ base: "flex-start", xs: "center" }}
        direction={{ base: "column-reverse", xs: "row" }}
        wrap={{ base: "nowrap", xs: "wrap-reverse" }}
        gap={10}
      >
        <Text
          c="customGray.4"
          fz={{ base: 14, xl: 18 }}
          fw="500"
          lh="1.17"
          ta={{ base: "start", xs: "center" }}
          mt={{ base: 20, xs: 0 }}
        >
          © Sales JobVerse. All rights reserved
        </Text>
        <Flex
          gap={{ base: 20, lg: 50 }}
          direction={{ base: "column", xs: "row" }}
          align={{ base: "flex-start", xs: "center" }}
        >
          <CustomLink href="/about" label="About" />
          <CustomLink href="/termsAndCondition" label="Terms & Conditions" />
          <a href="mailto:hello@salesjobverse.com">
          
              <EmailIconButton />
          
          </a>
          <a href={instagramLink} target="_blank">
            <InstagramIconButton />
          </a>
        </Flex>
      </Flex>
    </Box>
  );
};
export default Footer;
