import { Center } from "@mantine/core";
import { useRouter } from "next/router";
import { useEffect } from "react";
import Cookies from "js-cookie";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import ProfileLoader from "@components/loaders/ProfileLoader";
import { getHomePageForUserBasedOnRole } from "@/utils/navigation";

const loginToastId = "loginToastId";

const LoginPage = () => {
  const router = useRouter();
  const query = router.query;
  const token = String(query.token ?? "");
  const role = String(query.role ?? "");
  const redirectUrl = String(query.redirectUrl ?? "");
  const { showToast } = useCustomToast();

  useEffect(() => {
    if (!router.isReady) {
      return;
    }
    Cookies.set("userToken", token);
    showToast({
      status: ToastStatus.success,
      id: loginToastId,
      message: "Login successful",
    });
    if (redirectUrl) {
      router.replace(redirectUrl);
      return;
    }
    router.replace(getHomePageForUserBasedOnRole(role));
  }, [router.isReady]);
  return (
    <Center h="100vh" bg="customBlack.4">
      <ProfileLoader />
    </Center>
  );
};
export default LoginPage;
