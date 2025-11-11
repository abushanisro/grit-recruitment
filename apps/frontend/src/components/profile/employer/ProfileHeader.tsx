import { EmployerUserDataType } from "@/types/employer";
import YellowText from "@components/YellowText";
import IconWithLabel from "@components/jobs/jobcard/IconWithLabel";
import {
  Group,
  Stack,
  Text,
  Image,
  Divider,
  Tooltip,
  Flex,
  Center,
  Badge,
  Box,
} from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { IconPointFilled } from "@tabler/icons-react";
import { ProfileLinksSection } from "@/components/profile/jobSeeker/ProfileHeader";

export const NameText = ({ label }: { label: string }) => {
  return (
    <Text
      fz={{ base: 22, sm: 24, md: 28, lg: 32 }}
      fw={700}
      lh="1"
      lineClamp={2}
      maw={{ base: 300, sm: 400, md: 540 }}
      tt="capitalize"
    >
      {label}
    </Text>
  );
};

const ProfileHeader = ({ data }: { data: EmployerUserDataType }) => {
  const tablet = useMediaQuery("(max-width: 992px)");
  return (
    <Stack gap={tablet ? 18 : 20}>
      <ProfileLinksSection
        link1={{ link: null, tooltip: "" }}
        link2={{ link: data?.company.website, tooltip: "Website" }}
        link3={{
          link: data.verificationDocument,
          tooltip: "Verification Document",
        }}
      />

      <Group>
        <NameText label={`${data.firstName} ${data.lastName}`} />
        {data.isVerified ? (
          <Tooltip label="Your profile is verified">
            <Center c="secondarySkyBlue.4">
              <Image src="/images/blueTick.svg" h={20} w={20} alt="verified" />
            </Center>
          </Tooltip>
        ) : (
          <Badge fw={400} bg="secondaryGreen.1" c="black">
            Pending Verification
          </Badge>
        )}
      </Group>

      <Group gap={tablet ? 10 : 12} wrap="wrap">
        {data.city && (
          <>
            <IconWithLabel
              label={data.city.name}
              icon={
                <Image
                  src="/images/location.svg"
                  alt="location"
                  w={{ base: 16, md: 18 }}
                  h={{ base: 18, md: 20 }}
                />
              }
              fontProps={{ fz: { base: 16, md: 20, xl: 24 } }}
              gap={6}
            />
            <Divider orientation="vertical" h={30} color="secondaryGreen.1" />
          </>
        )}

        <YellowText label={data.email} />
        <Box ml={-5} mt={7}>
          <IconPointFilled
            style={{ color: "var(--mantine-color-secondarySkyBlue-4)" }}
          />
        </Box>
        <YellowText ml="-10" label={String(data.phone)} />
      </Group>
      <Flex wrap="wrap" gap={tablet ? 14 : 20} align="center">
        <Text fw={400} fz={{ base: 16, xl: 22 }} lh="1.17" c="secondaryGreen.1">
          Company Name
        </Text>
        <Divider orientation="vertical" h={30} />
        <Text
          fz={{ base: 16, xl: 22 }}
          fw={400}
          lh="1.17"
          lineClamp={2}
          c="white"
        >
          {data.company.name}
        </Text>
      </Flex>
    </Stack>
  );
};
export default ProfileHeader;
