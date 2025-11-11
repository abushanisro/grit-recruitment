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
        dropdown: { borderColor: "var(--mantine-color-secondaryRed-4)" },
      }}
    >
      <Menu.Target>
        <Group style={{ cursor: "pointer" }}>
          {profilePicture ? (
            <Avatar
              src={profilePicture}
              style={{
                cursor: "pointer",
                border: "0.1px solid var(--mantine-color-secondaryYellow-4)",
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
                    <IconClockFilled style={{ color: "#FC6977" }} size={16} />
                  </Tooltip>
                ))}
            </Group>
            <Text
              c="primaryOrange.4"
              fz={{ base: 12, sm: 12 }}
              fw={400}
              lh="1.17"
            >
              {getUserRoleLabel(userDetails?.role ?? "")}
            </Text>
          </Stack>
        </Group>
      </Menu.Target>

      <Menu.Dropdown bg="customBlack.4">
        <Menu.Item
          c="white"
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
