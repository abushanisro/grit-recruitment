import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import Cookies from "js-cookie";
import isEmpty from "lodash/isEmpty";
import { useRouter } from "next/router";
import { createContext, useContext, useEffect, useState } from "react";

const logoutToastId = "logoutToastId";

const loginRestrictedPages: string[] = ["/uploadJobs/", "/deleteJobs"];

interface ContextInterface {
  isLoggedIn: boolean;
  refreshLoginState: () => void;
}

const LoginContext = createContext<ContextInterface>({
  isLoggedIn: false,
  refreshLoginState: () => {},
});

export const LoginProvider = ({ children }: { children: React.ReactNode }) => {
  const router = useRouter();
  const { showToast } = useCustomToast();
  const [isLoggedIn, setIsLoggedIn] = useState<boolean>(false);

  const refreshLoginState = () => {
    const token = Cookies.get("userToken");
    setIsLoggedIn(!isEmpty(token));
  };
  useEffect(() => {
    refreshLoginState();
  }, [router.asPath]);

  useEffect(() => {
    if (Cookies.get("userToken")) {
      return;
    }
    if (
      loginRestrictedPages.some((path: string) => router.asPath.includes(path))
    ) {
      showToast({
        status: ToastStatus.error,
        id: logoutToastId,
        message: "Please log in to view this page.",
      });
      router.push("/");
    }
  }, [isLoggedIn, router.asPath]);

  return (
    <LoginContext.Provider
      value={{
        isLoggedIn,
        refreshLoginState,
      }}
    >
      {children}
    </LoginContext.Provider>
  );
};

export const useLogin = () => useContext(LoginContext);
