import { Stack } from "@mantine/core";
import { useLogin } from "@/contexts/LoginProvider";
import CustomButton from "@components/buttons/CustomButton";
import { useRouter } from "next/router";
import Cookies from "js-cookie";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import NotLoggedInNavbar from "@/components/NotLoggedInNavbar";

const logoutToastId = "logoutToastId";

const Navbar = ({ children }: { children?: React.ReactElement }) => {
  const { showToast } = useCustomToast();
  const { isLoggedIn, refreshLoginState } = useLogin();
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
  return (
    <Stack px={10} py={20} style={{ overflow: "auto" }}>
      {isLoggedIn ? (
        <CustomButton
          label="Logout"
          mih={40}
          c="secondaryYellow.3"
          bg="black"
          styles={{
            root: {
              border: "1px solid",
              borderColor: "var(--mantine-color-secondaryYellow-3)",
            },
          }}
          w="100%"
          onClick={handleLogout}
        />
      ) : (
        <NotLoggedInNavbar />
      )}

      {children}
    </Stack>
  );
};
export default Navbar;
