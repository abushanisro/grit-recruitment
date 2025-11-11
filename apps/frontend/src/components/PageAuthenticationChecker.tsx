import { useRouter } from "next/router";
import { useEffect } from "react";
import { useLogin } from "@/contexts/LoginProvider";
import { useUserData } from "@/contexts/UserProvider";
import { UserRole } from "contract/enum";
import {
  employerPagePatterns,
  isPageMatchingAnyPattern,
  jobSeekerPagePatterns,
  noCheckPagePatterns,
} from "@/utils/pageRestrictions";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { useAnalyticsEvent } from "@/hooks/useAnalyticsEvent";
import { getEnv } from "@/env";
import { getHomePageForUserBasedOnRole } from "@/utils/navigation";

const authenticationToastId = "authenticationToastId";

const rolePagePatterns: Record<UserRole, RegExp[]> = {
  [UserRole.jobSeeker]: jobSeekerPagePatterns,
  [UserRole.employer]: employerPagePatterns,
  [UserRole.admin]: [],
};

export const PageAuthenticationChecker = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const router = useRouter();
  const { isLoggedIn } = useLogin();
  const { userDetails } = useUserData();
  const { showToast } = useCustomToast();
  const { sendEvent } = useAnalyticsEvent();

  useEffect(() => {
    if (!isLoggedIn || !userDetails) {
      import("posthog-js").then((posthog) => {
        posthog.default.reset();
      });
      return;
    }

    if (
      router.asPath === "/" ||
      isPageMatchingAnyPattern({
        currentPageAsPath: router.asPath,
        pagePatterns: noCheckPagePatterns,
      })
    )
      return;

    const allowedPatterns = rolePagePatterns[userDetails.role];

    if (
      !isPageMatchingAnyPattern({
        currentPageAsPath: router.asPath,
        pagePatterns: allowedPatterns,
      })
    ) {
      showToast({
        status: ToastStatus.error,
        id: authenticationToastId,
        message: "You do not have authorization to view this page.",
      });
      router.push("/");
    }
  }, [router.asPath, userDetails]);

  useEffect(() => {
    if (!isLoggedIn) {
      return;
    }
    sendEvent("PageView");
  }, [router.asPath]);

  useEffect(() => {
    if (!isLoggedIn || !userDetails) {
      return;
    }
    const handleRouteChange = (url: string) => {
      if (url.includes("login") || url.includes("signup")) {
        router.push(getHomePageForUserBasedOnRole(userDetails.role));
      }
    };
    router.events.on("routeChangeStart", handleRouteChange);
    return () => {
      router.events.off("routeChangeStart", handleRouteChange);
    };
  }, [router]);

  useEffect(() => {
    if (!isLoggedIn || !userDetails) {
      import("posthog-js").then((posthog) => {
        posthog.default.reset();
      });
      return;
    }

    import("posthog-js").then((posthog) => {
      posthog.default.identify(
        userDetails?.email, // 'distinct_id'  user's unique identifier
        { userDetails, environment: getEnv() } // optional: additional user properties
      );
    });
  }, [router.asPath]);

  return <>{children}</>;
};
