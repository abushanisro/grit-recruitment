import { JobSeekerUserDataType } from "@/types/jobSeeker";
import YellowText from "@components/YellowText";
import PrimaryIconButton from "@components/buttons/PrimaryIconButton";
import SpacedOutText from "@components/jobs/jobId/SpacedOutText";
import IconWithLabel from "@components/jobs/jobcard/IconWithLabel";
import {
  ActionIcon,
  Group,
  Stack,
  Image,
  Divider,
  Tooltip,
  Box,
} from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { IconPointFilled } from "@tabler/icons-react";
import Link from "next/link";
import { NameText } from "@/components/profile/employer/ProfileHeader";
import classes from "styles/profileFile.module.css";

export const ProfileLinksSection = ({
  link1,
  link2,
  link3,
  
}: {
  link1: { link: string | null; tooltip: string };
  link2: { link: string | null; tooltip: string };
  link3: { link: string | null; tooltip: string };
  
}) => {
  const tablet = useMediaQuery("(max-width: 768px)");

  return (
    <Group justify="flex-end" align="center" gap={tablet ? 10 : 30}>
      {link1.link && (
        <Tooltip label={link1.tooltip}>
          <ActionIcon
            variant="transparent"
            component="a"
            href={link1.link}
            target="_blank"
          >
            <Image
              src="/images/profileLink.svg"
              className={classes.actionIcon}
              alt="icon"
              w={{ base: 24, sm: 36, md: 48 }}
              h={{ base: 24, sm: 36, md: 48 }}
            />
          </ActionIcon>
        </Tooltip>
      )}

      {link2.link && (
        <Tooltip label={link2.tooltip}>
          <ActionIcon
            variant="transparent"
            component="a"
            href={link2.link}
            target="_blank"
          >
            <Image
              src="/images/profileFeed.svg"
              className={classes.actionIcon}
              alt="icon"
              w={{ base: 24, sm: 36, md: 48 }}
              h={{ base: 24, sm: 36, md: 48 }}
            />
          </ActionIcon>
        </Tooltip>
      )}
      {link3.link && (
        <Tooltip label={link3.tooltip}>
          <Link
            href={link3.link}
            target="_blank"
            style={{ height: "fit-content" }}
          >
            <PrimaryIconButton
              bg="secondarySkyBlue.4"
              ml={{ base: 0, lg: -10 }}
              className={classes.profileFile}
              style={{
                border: "1px solid",
                borderColor: "var(--mantine-color-secondarySkyBlue-4)",
                borderRadius: "100%",
              }}
              w={{ base: 24, sm: 36, md: 48 }}
              h={{ base: 24, sm: 36, md: 48 }}
            >
              <Image
                src="/images/profileFile.svg"
                c="black"
                alt="icon"
                w={{ base: 14, sm: 23 }}
                h={{ base: 14, sm: 23 }}
              />
            </PrimaryIconButton>
          </Link>
        </Tooltip>
      )}
      
    </Group>
  );
};

const ProfileHeader = ({ data }: { data: JobSeekerUserDataType }) => {
  const tablet = useMediaQuery("(max-width: 992px)");

  return (
    <Stack gap={tablet ? 18 : 20}>
      <ProfileLinksSection
        link1={{ link: data.externalLink, tooltip: "Portfolio link" }}
        link2={{ link: data.socialMediaLink, tooltip: "Social link" }}
        link3={{ link: data.resume, tooltip: "Resume" }}

      />
      <NameText label={`${data.firstName} ${data.lastName}`} />
      <SpacedOutText label={data.headline} mb={0} />
      <Group wrap="wrap">
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
          gap={10}
        />
        <Divider orientation="vertical" h={30} color="secondaryGreen.1" />

        <YellowText label={data.email} />
        <Box ml={-5} mt={7}>
          <IconPointFilled
            style={{
              color: "var(--mantine-color-secondarySkyBlue-4)",
            }}
          />
        </Box>
        <YellowText  label={String(data.phone)} />
      </Group>
    </Stack>
  );
};
export default ProfileHeader;
