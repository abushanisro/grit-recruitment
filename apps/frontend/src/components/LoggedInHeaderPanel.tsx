import { useRouter } from "next/router";
import ProfileMenuButton from "@/components/ProfileMenuButton";
import { useUserData } from "@/contexts/UserProvider";
import { Group, Menu, Tooltip, Text, Button } from "@mantine/core";
import {
  IconAlertCircleFilled,
  IconCaretUpFilled,
  IconLock,
} from "@tabler/icons-react";
import PrimaryIconButton from "@/components/buttons/PrimaryIconButton";
import { getMenuForUserBasedOnRole } from "@/utils/navigation";
import { Fragment } from "react";
import NavLinkButton from "@/components/buttons/NavLinkButton";
import classes from "styles/menu.module.css";
import PaymentConfirmationModal from "@/components/jobSeekerDirectory/PaymentConfirmationModal";
import { useDisclosure } from "@mantine/hooks";
import { UserRole } from "contract/enum";

const LoggedInHeaderPanel = () => {
  const router = useRouter();
  const [opened, { close }] = useDisclosure(false);
  const { userDetails } = useUserData();
  const handleClick = () => {
    router.push("/subscription");
    close();
  };

  const navLinkStyles = {
    color: "var(--mantine-color-primarySkyBlue-6)",
    backgroundColor: "var(--mantine-color-primaryDarkBlue-9)",
  };

  const menuButtonStyles = {
    color: "var(--mantine-color-primarySkyBlue-6)",
    backgroundColor: "var(--mantine-color-primaryDarkBlue-9)",
    border: "1px solid",
    borderColor: "var(--mantine-color-primarySkyBlue-6)",
  };

  if (!userDetails) {
    return (
      <PrimaryIconButton>
        <Tooltip label="Profile data not found">
          <IconAlertCircleFilled
            size={32}
            color="var(--mantine-color-primarySkyBlue-6)"
            aria-label="profile icon not found"
          />
        </Tooltip>
      </PrimaryIconButton>
    );
  }

  if (userDetails.role === UserRole.employer) {
    return (
      <>
        <PaymentConfirmationModal
          opened={opened}
          onClose={close}
          modalHeader={`Please subscribe to our plans to view this page`}
          onSuccessFn={handleClick}
          successButtonLabel="View Plans"
          secondaryButtonLabel="Cancel"
        />
        <Fragment>
          <Menu
            width={160}
            styles={{
              dropdown: {
                borderColor: "var(--mantine-color-primarySkyBlue-6)",
                backgroundColor: "var(--mantine-color-primaryDarkBlue-9)",
              },
            }}
            closeOnClickOutside={true}
          >
            <Menu.Target>
              <Button
                radius={10}
                py={6}
                h={40}
                className={classes.menuBar}
                c="primarySkyBlue.6"
                fz={{ base: 14, md: 16 }}
                lh={{ md: "1.2", lg: "1.17" }}
                styles={{
                  root: menuButtonStyles,
                }}
                fw="500"
                rightSection={
                  <IconCaretUpFilled
                    color={"var(--mantine-color-primarySkyBlue-6)"}
                    style={{
                      transform: `rotate(180deg)`,
                    }}
                    size={18}
                  />
                }
              >
                Jobs
              </Button>
            </Menu.Target>
            <Menu.Dropdown>
              <NavLinkButton
                label={"Post a Job"}
                w="100%"
                justify="flex-start"
                pageLink={"/postJob"}
                style={{ ...navLinkStyles, borderColor: "transparent" }}
                isCurrentPage={router.pathname === "/postJob"}
              />
              <NavLinkButton
                label={"Manage Jobs"}
                w="100%"
                justify="flex-start"
                style={{ ...navLinkStyles, borderColor: "transparent" }}
                pageLink={"/manageJobs"}
                isCurrentPage={router.pathname === "/manageJobs"}
              />
            </Menu.Dropdown>
          </Menu>

          <Menu
            width={160}
            styles={{
              dropdown: {
                borderColor: "var(--mantine-color-primarySkyBlue-6)",
                backgroundColor: "var(--mantine-color-primaryDarkBlue-9)",
              },
            }}
            closeOnClickOutside={false}
          >
            <Menu.Target>
              <Button
                radius={10}
                py={6}
                h={40}
                className={classes.menuBar}
                c="primarySkyBlue.6"
                fz={{ base: 14, md: 16 }}
                lh={{ md: "1.2", lg: "1.17" }}
                styles={{
                  root: menuButtonStyles,
                }}
                fw="500"
                rightSection={
                  <IconCaretUpFilled
                    color={"var(--mantine-color-primarySkyBlue-6)"}
                    style={{
                      transform: `rotate(180deg)`,
                    }}
                    size={18}
                  />
                }
              >
                Discover Talent
              </Button>
            </Menu.Target>
            <Menu.Dropdown>
              <NavLinkButton
                label={
                  <Group gap={6}>
                    <Text fz={{ base: 14, md: 16, xl: 16 }} fw="500">
                      Jobseeker
                    </Text>
                    {userDetails && !userDetails.isPurchaseActive && (
                      <IconLock size={"18"} />
                    )}
                  </Group>
                }
                w="100%"
                justify="flex-start"
                style={{ ...navLinkStyles, borderColor: "transparent" }}
                pageLink="/jobseekerDirectoryFilter"
                isCurrentPage={router.pathname === "/jobseekerDirectoryFilter"}
                className={classes.menuBar}
              />
            </Menu.Dropdown>
          </Menu>
        </Fragment>
        <ProfileMenuButton profilePicture={userDetails.picture ?? null} />
      </>
    );
  }

  return (
    <>
      {getMenuForUserBasedOnRole({
        role: userDetails.role,
        pathname: router.pathname,
      }).map((eachNavObj, index) => (
        <Fragment key={index}>
          <NavLinkButton
            label={eachNavObj.label}
            pageLink={eachNavObj.pageLink}
            style={navLinkStyles}
            isCurrentPage={eachNavObj.isCurrentPage}
          />
        </Fragment>
      ))}
      <ProfileMenuButton profilePicture={userDetails.picture ?? null} />
    </>
  );
};

export default LoggedInHeaderPanel;
