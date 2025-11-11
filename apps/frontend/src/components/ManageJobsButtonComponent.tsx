import { Button, Flex, Menu, Stack, Text } from "@mantine/core";
import { IconEye, IconTrash, IconTrendingUp2 } from "@tabler/icons-react";
import Link from "next/link";
import React from "react";
import PrimaryButton from "./buttons/PrimaryButton";
import classes from "styles/menu.module.css";
import { useMediaQuery } from "@mantine/hooks";

const ManageJobsButtonComponent = ({
  isJobPendingStatus,
  isPromoted,
  href,
  isJobInDraftStatus,
  isJobPosted,
  open,
  handlePromoteJob,
  postJobOpen,
}: {
  isJobPendingStatus: boolean;
  isPromoted: boolean;
  href: string;
  isJobInDraftStatus: boolean;
  isJobPosted: false | Date | null;
  open: () => void;
  handlePromoteJob: () => void;
  postJobOpen: () => void;
}) => {
  const isTablet = useMediaQuery("(max-width: 768px)");

  return (
    <div>
      <Stack w={{ base: "100%", sm: "max-content" }}>
        {isJobPosted && isPromoted && (
          <>
            <Link
              href={href}
              style={{ width: isTablet ? "100%" : "max-content" }}
            >
              <PrimaryButton
                label="View Applicants"
                w="100%"
                maw={{ base: "100%", sm: "max-content" }}
                leftSection={<IconEye />}
              />
            </Link>
            <PrimaryButton
              label="Delete Job Post"
              bg="secondaryGreen.1"
              onClick={open}
              className={classes.cardButton}
              leftSection={<IconTrash size={22} />}
              w="100%"
              maw={{ base: "100%", sm: "max-content" }}
            />
          </>
        )}
        {isJobPosted && !isPromoted && (
          <>
            <PrimaryButton
              label="Promote job post"
              w="100%"
              onClick={handlePromoteJob}
              maw={{ base: "100%", sm: "max-content" }}
              leftSection={<IconTrendingUp2 />}
            />
            <Menu
              width={200}
              styles={{
                dropdown: {
                  borderColor: "var(--mantine-color-secondarySkyBlue-4)",
                },
              }}
            >
              <Menu.Target>
                <Button
                  bg="secondaryGreen.1"
                  c="secondaryDarkBlue.9"
                  w="100%"
                  radius={10}
                  className={classes.cardButton}
                  px={{ base: 10, xl: 20 }}
                  py={10}
                  fz={{ base: 14, md: 16, xl: 20 }}
                  maw={{ base: "100%", sm: "max-content" }}
                >
                  + More actions
                </Button>
              </Menu.Target>

              <Menu.Dropdown bg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)">
                <Link
                  href={href}
                  style={{
                    width: isTablet ? "100%" : "max-content",
                    textDecoration: "none",
                  }}
                >
                  <Menu.Item c="secondaryGreen.1" bg="transparent">
                    <Flex gap={6}>
                      <IconEye size={20} />
                      <Text fw={600} fz={14}>
                        View Applicants
                      </Text>
                    </Flex>
                  </Menu.Item>
                </Link>
                <Menu.Item
                  c="secondaryGreen.1"
                  onClick={open}
                  bg="transparent"
                  fw={600}
                >
                  <Flex gap={6}>
                    <IconTrash size={18} />
                    <Text fw={600} fz={14}>
                      Delete Job Post
                    </Text>
                  </Flex>
                </Menu.Item>
              </Menu.Dropdown>
            </Menu>
          </>
        )}

        {isJobInDraftStatus && isPromoted && (
          <>
            <PrimaryButton
              label="Post Job"
              onClick={postJobOpen}
              w="100%"
              maw={{ base: "100%", sm: "max-content" }}
            />
            <PrimaryButton
              label="Delete Job Post"
              className={classes.cardButton}
              onClick={open}
              w="100%"
              leftSection={<IconTrash size={22} />}
              maw={{ base: "100%", sm: "max-content" }}
            />
          </>
        )}
        {isJobInDraftStatus && !isPromoted && (
          <>
            <PrimaryButton
              label="Promote job post"
              onClick={handlePromoteJob}
              w="100%"
              maw={{ base: "100%", sm: "max-content" }}
              leftSection={<IconTrendingUp2 />}
            />
            <Menu
              styles={{
                dropdown: {
                  borderColor: "var(--mantine-color-secondarySkyBlue-4)",
                },
              }}
            >
              <Menu.Target>
                <Button
                  bg="secondaryGreen.1"
                  c="secondaryDarkBlue.9"
                  w="100%"
                  radius={10}
                  className={classes.cardButton}
                  px={{ base: 10, xl: 20 }}
                  py={10}
                  fz={{ base: 14, md: 16, xl: 20 }}
                  maw={{ base: "100%", sm: "max-content" }}
                >
                  + More actions
                </Button>
              </Menu.Target>

              <Menu.Dropdown bg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)">
                <Menu.Item
                  c="secondaryGreen.1"
                  onClick={postJobOpen}
                  fw={600}
                  bg="transparent"
                >
                  Post Job
                </Menu.Item>
                <Menu.Item
                  c="secondaryGreen.1"
                  onClick={open}
                  bg="transparent"
                  fw={600}
                >
                  <Flex gap={6}>
                    <IconTrash size={18} />
                    <Text fw={600} fz={14}>
                      Delete Job Post
                    </Text>
                  </Flex>
                </Menu.Item>
              </Menu.Dropdown>
            </Menu>
          </>
        )}
        {isJobPendingStatus && !isPromoted && (
          <>
            <PrimaryButton
              label="Promote job post"
              onClick={handlePromoteJob}
              w="100%"
              maw={{ base: "100%", sm: "max-content" }}
              leftSection={<IconTrendingUp2 />}
            />
            <PrimaryButton
              label="Delete Job Post"
              bg="secondaryGreen.1"
              c="secondaryDarkBlue.9"
              className={classes.cardButton}
              leftSection={<IconTrash size={22} />}
              onClick={open}
              w="100%"
              maw={{ base: "100%", sm: "max-content" }}
            />
          </>
        )}
        {isJobPendingStatus && isPromoted && (
          <PrimaryButton
            label="Delete Job Post"
            bg="secondaryGreen.1"
            c="secondaryDarkBlue.9"
            className={classes.cardButton}
            leftSection={<IconTrash size={22} />}
            onClick={open}
            w="100%"
            maw={{ base: "100%", sm: "max-content" }}
          />
        )}
      </Stack>
    </div>
  );
};

export default ManageJobsButtonComponent;
