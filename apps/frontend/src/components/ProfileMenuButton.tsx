import {
  Menu,
  Image,
  Avatar,
  Group,
  Text,
  Stack,
  Tooltip,
} from "@mantine/core";
import PrimaryIconButton from "@/components/buttons/PrimaryIconButton";
import { forwardRef } from "react";
import Cookies from "js-cookie";
import { useLogin } from "@/contexts/LoginProvider";
import { useRouter } from "next/router";
import Link from "next/link";
import classes from "styles/menu.module.css";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { useUserData } from "@/contexts/UserProvider";
import { UserRole } from "contract/enum";
import { IconClockFilled } from "@tabler/icons-react";

const logoutToastId = "logoutToastId";

const getUserRoleLabel = (role: string) => {
  switch (role) {
    case UserRole.jobSeeker:
      return "Jobseeker";
    case UserRole.employer:
      return "Employer";
    case UserRole.admin:
      return "Admin";
    default:
      return "";
  }
};

const ProfileButton = forwardRef<
  HTMLDivElement,
  React.ComponentPropsWithoutRef<"div">
>((props, ref) => (
  <div ref={ref} {...props}>
    <PrimaryIconButton bg="black" aria-label="profile icon">
      <Image src="/images/profile.svg" alt="profile icon" w={32} />
    </PrimaryIconButton>
  </div>
));
ProfileButton.displayName = "ProfileButton";

const ProfileMenuButton = ({
  profilePicture,
}: {
  profilePicture: string | null;
}) => {
  const { refreshLoginState } = useLogin();
  const { showToast } = useCustomToast();
  const router = useRouter();
  const handleLogout = () => {
    Cookies.remove("userToken");
    refreshLoginState();
    showToast({
      status: ToastStatus.success,
      id: logoutToastId,
      message: "Logout successful.",
    });
    router.push("/");
  };
  const { userDetails } = useUserData();
  return (
    <Menu
      shadow="md"
      width="target"
      styles={{
        dropdown: {
          borderColor: "var(--mantine-color-primarySkyBlue-6)",
          background: "transparent",
        },
      }}
    >
      <Menu.Target>
        <Group style={{ cursor: "pointer" }}>
          {profilePicture ? (
            <Avatar
              src={profilePicture}
              style={{
                cursor: "pointer",
                border: "1px solid var(--mantine-color-primarySkyBlue-6)",
              }}
              alt="profile picture"
            />
          ) : (
            <ProfileButton />
          )}
          <Stack gap={2}>
            <Group gap={5}>
              <Text
                c="white"
                fz={{ base: 12, sm: 16 }}
                fw={600}
                lh="1.17"
                maw={100}
                truncate
              >
                {userDetails?.firstName}{" "}
              </Text>
              {userDetails?.role === UserRole.employer &&
                (userDetails?.isVerified ? (
                  <Tooltip label="Your profile is verified">
                    <Image
                      src="/images/blueTick.svg"
                      h={16}
                      w={16}
                      alt="verified"
                    />
                  </Tooltip>
                ) : (
                  <Tooltip label="Verification pending">
                    <IconClockFilled style={{ color: "#FC6977" }} size={14} />
                  </Tooltip>
                ))}
            </Group>
            <Text
              c="primarySkyBlue.6"
              fz={{ base: 12, sm: 12 }}
              fw={400}
              lh="1.17"
            >
              {getUserRoleLabel(userDetails?.role ?? "")}
            </Text>
          </Stack>
        </Group>
      </Menu.Target>

      <Menu.Dropdown
        bg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
        miw={200}
      >
        <Link href="/profile" style={{ textDecoration: "none" }}>
          <Menu.Item
            c="primarySkyBlue.6"
            fz={{ base: 14, xl: 18 }}
            fw="400"
            lh="1.17"
            className={classes.customMenu}
          >
            Profile
          </Menu.Item>
        </Link>
        {userDetails &&
          userDetails.role == UserRole.employer &&
          (userDetails.isPurchaseActive ||
            userDetails.isJobBoostPurchaseActive) && (
            <>
              <Link href="/subscription" style={{ textDecoration: "none" }}>
                <Menu.Item
                  c="primarySkyBlue.6"
                  fz={{ base: 14, xl: 18 }}
                  fw="400"
                  lh="1.17"
                  className={classes.customMenu}
                >
                  Subscription Plans
                </Menu.Item>
              </Link>
              <Link href="/postPaidPlans" style={{ textDecoration: "none" }}>
                <Menu.Item
                  c="primarySkyBlue.6"
                  fz={{ base: 14, xl: 18 }}
                  fw="400"
                  lh="1.17"
                  className={classes.customMenu}
                >
                  Post Paid Plans
                </Menu.Item>
              </Link>
              <Link
                href="/subscriptionDetails"
                style={{ textDecoration: "none" }}
              >
                <Menu.Item
                  c="primarySkyBlue.6"
                  fz={{ base: 14, xl: 18 }}
                  fw="400"
                  lh="1.17"
                  className={classes.customMenu}
                >
                  My Subscription
                </Menu.Item>
              </Link>
              <Link href="/payment" style={{ textDecoration: "none" }}>
                <Menu.Item
                  c="primarySkyBlue.6"
                  fz={{ base: 14, xl: 18 }}
                  fw="400"
                  lh="1.17"
                  className={classes.customMenu}
                >
                  My Payment
                </Menu.Item>
              </Link>
            </>
          )}

        <Menu.Item
          c="primarySkyBlue.6"
          onClick={handleLogout}
          fz={{ base: 14, xl: 18 }}
          className={classes.customMenu}
        >
          Log out
        </Menu.Item>
      </Menu.Dropdown>
    </Menu>
  );
};

export default ProfileMenuButton;
