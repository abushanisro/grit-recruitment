import { Stack } from "@mantine/core";
import { useLogin } from "@/contexts/LoginProvider";
import Link from "next/link";
import CustomButton from "@components/buttons/CustomButton";
import { useRouter } from "next/router";
import Cookies from "js-cookie";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import LoggedInNavbar from "@/components/LoggedInNavbar";
import NotLoggedInNavbar from "@/components/NotLoggedInNavbar";

const logoutToastId = "logoutToastId";

const Navbar = ({ children }: { children?: React.ReactElement }) => {
  const { showToast } = useCustomToast();
  const { isLoggedIn, refreshLoginState } = useLogin();
  const router = useRouter();
  const isCurrentPage = router.pathname.includes("/profile");
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
  return (
    <Stack px={10} py={20} style={{ overflow: "auto" }}>
      {isLoggedIn ? (
        <>
          <Link href="/profile">
            <CustomButton
              label="Profile"
              c={isCurrentPage ? "primaryDarkBlue.9" : "primarySkyBlue.6"}
              bg={isCurrentPage ? "primarySkyBlue.6" : "primaryDarkBlue.9"}
              styles={{
                root: {
                  border: "1px solid",
                  borderColor: "var(--mantine-color-primarySkyBlue-6)",
                },
              }}
              w="100%"
            />
          </Link>
          <LoggedInNavbar />
          <CustomButton
            label="Logout"
            c="primarySkyBlue.6"
            mih={40}
            bg="primaryDarkBlue.9"
            styles={{
              root: {
                border: "1px solid",
                borderColor: "var(--mantine-color-primarySkyBlue-6)",
              },
            }}
            w="100%"
            onClick={handleLogout}
          />
        </>
      ) : (
        <NotLoggedInNavbar />
      )}

      {children}
    </Stack>
  );
};
export default Navbar;
