import {
  ButtonProps,
  Center,
  Group,
  Image,
  Tooltip,
  Box,
  Button,
} from "@mantine/core";
import { useHover } from "@mantine/hooks";
import isNil from "lodash/isNil";
import classes from "styles/profileFile.module.css";

const ManageCardActions = ({
  showCoverLetterButton,
  onCoverLetterOpen,
  resumeUrl,
  hasDownload = true,
  onShortlist,
  onReject,
  videoResumeUrl,
}: {
  showCoverLetterButton: boolean;
  onCoverLetterOpen: () => void;
  resumeUrl: string | null;
  onShortlist: () => void;
  onReject: () => void;
  hasDownload?: boolean;
  videoResumeUrl: string | null;
}) => {
  return (
    <>
      <Group>
        {!isNil(resumeUrl) && (
          <a
            href={resumeUrl}
            target="_blank"
            download={hasDownload}
            rel="noreferrer"
          >
            <Tooltip label="Resume">
              <Box
                w={{ base: 40, sm: 52 }}
                h={{ base: 40, sm: 52 }}
                className={classes.actionIcon}
                style={{
                  borderRadius: "50%",
                  border: "1px solid",
                  borderColor: "var(--mantine-color-primarySkyBlue-4)",
                }}
              >
                <Center h="100%">
                  <Image src="/images/resumeFile.svg" alt="" w={18} h={21} />
                </Center>
              </Box>
            </Tooltip>
          </a>
        )}
        {!isNil(videoResumeUrl) && (
          <a
            href={`/playVideoResume/?videoUrl=${videoResumeUrl}`}
            target="_blank"
            rel="noreferrer"
          >
            <Tooltip label="Video Resume">
              <Box
                w={{ base: 40, sm: 52 }}
                h={{ base: 40, sm: 52 }}
                className={classes.actionIcon}
                style={{
                  borderRadius: "50%",
                  border: "1px solid",
                  borderColor: "var(--mantine-color-primarySkyBlue-4)",
                }}
              >
                <Center h="100%">
                  <Image src="/images/videoResume.svg" alt="" w={18} h={21} />
                </Center>
              </Box>
            </Tooltip>
          </a>
        )}
        {showCoverLetterButton && (
          <Tooltip label="View Cover Letter">
            <Center
              onClick={onCoverLetterOpen}
              w={{ base: 40, sm: 52 }}
              h={{ base: 40, sm: 52 }}
              className={classes.actionIcon}
              style={{
                cursor: "pointer",
                borderRadius: "50%",
                border: "1px solid",
                borderColor: "var(--mantine-color-primarySkyBlue-6)",
              }}
            >
              <Image src="/images/coverLetter.svg" alt="" w={18} h={21} />
            </Center>
          </Tooltip>
        )}
      </Group>
      <Group wrap="nowrap">
        <ShortlistButton
          fw={600}
          radius={8}
          onClick={onShortlist}
          style={{
            border: "1px solid var(--mantine-color-primarySkyBlue-6)",
          }}
        >
          Shortlist
        </ShortlistButton>
        <Tooltip label="Reject">
          <Box
            p={9}
            onClick={onReject}
            style={{
              border: "1px solid var(--mantine-color-secondaryGreen-1)",
              borderRadius: 8,
              cursor: "pointer",
            }}
          >
            <Image src="/images/reject.svg" alt="reject" w={20} h={20} />
          </Box>
        </Tooltip>
      </Group>
    </>
  );
};

const ShortlistButton = ({
  onClick,
  ...props
}: { onClick: () => void } & ButtonProps) => {
  const { hovered, ref } = useHover<HTMLButtonElement>();
  return (
    <Button
      ref={ref}
      radius={10}
      px={{ base: 10, xl: 20 }}
      py={6}
      h={40}
      fz={{ base: 16, xl: 20 }}
      lh={{ md: "1.2", lg: "1.17" }}
      c={
        hovered
          ? "var(--mantine-color-primarySkyBlue-6)"
          : "var(--mantine-color-primaryDarkBlue-9)"
      }
      fw="500"
      onClick={onClick}
      bg={hovered ? "transparent" : "var(--mantine-color-primarySkyBlue-6)"}
      {...props}
    />
  );
};

export default ManageCardActions;
