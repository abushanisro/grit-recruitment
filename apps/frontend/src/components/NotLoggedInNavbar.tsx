import { getFrontendUrl } from "@/env";
import {
  loginViaGoogle,
  loginViaGoogleWithRedirectTarget,
} from "@/utils/common";
import Link from "next/link";
import { useRouter } from "next/router";
import CustomButton from "@/components/buttons/CustomButton";

const LoginButtons = () => {
  const router = useRouter();
  return (
    <>
      <CustomButton
        label="Sign up"
        bg="primarySkyBlue.6"
        c="black"
        styles={{
          root: {
            border: "1px solid",
            borderColor: "var(--mantine-color-primarySkyBlue-6)",
          },
        }}
        w="100%"
        onClick={() => loginViaGoogle(router.pathname)}
      />
      <CustomButton
        label="Login"
        c="black"
        bg="primarySkyBlue.6"
        styles={{
          root: {
            border: "1px solid",
            borderColor: "var(--mantine-color-primarySkyBlue-6)",
          },
        }}
        w="100%"
        onClick={() => loginViaGoogle(router.pathname)}
      />
    </>
  );
};
const NotLoggedInNavbar = () => {
  const router = useRouter();
  const isEmployerHomePage = router.pathname.includes("/employer");

  if (isEmployerHomePage) {
    return (
      <>
        <CustomButton
          label="Post Job"
          styles={{
            root: {
              border: "1px solid",
              borderColor: "var(--mantine-color-primaryGreen-6)",
            },
          }}
          onClick={() =>
            loginViaGoogleWithRedirectTarget({
              pathname: router.pathname,
              redirectUrl: `${getFrontendUrl()}/postJob`,
            })
          }
          w="100%"
        />
        <Link href="/manageJobs" style={{ textDecoration: "none" }}>
          <CustomButton
            label="ManageJobs"
            styles={{
              root: {
                border: "1px solid",
                borderColor: "var(--mantine-color-primarySkyBlue-6)",
              },
            }}
            w="100%"
          />
        </Link>
        <LoginButtons />
      </>
    );
  }
  return (
    <>
      <LoginButtons />
    </>
  );
};
export default NotLoggedInNavbar;
