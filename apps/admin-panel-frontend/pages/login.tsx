import { Center } from "@mantine/core";
import { useRouter } from "next/router";
import { useEffect } from "react";
import Cookies from "js-cookie";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import ProfileLoader from "@components/loaders/ProfileLoader";

const loginToastId = "loginToastId";

const LoginPage = () => {
  const router = useRouter();
  const query = router.query;
  const token = String(query.token ?? "");
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
      router.push(redirectUrl);
      return;
    }
    router.push("/");
  }, [router.isReady]);
  return (
    <Center h="100vh" bg="customBlack.4">
      <ProfileLoader />
    </Center>
  );
};
export default LoginPage;
