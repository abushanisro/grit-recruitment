import { useRouter } from "next/router";
import SecondaryButton from "@/components/buttons/SecondaryButton";
import {
  loginViaGoogle,
  loginViaGoogleWithRedirectTarget,
} from "@/utils/common";
import PrimaryButton from "@/components/buttons/PrimaryButton";
import { getFrontendUrl, instagramLink } from "@/env";
import InstagramIconButton from "@/components/socialMediaButtons/InstagramIconButton";
import NotLoggedInNavLinkButton from "@/components/buttons/NotLoggedInNavLinkButton";

const NotLoggedInHeaderPanel = () => {
  const router = useRouter();
  const isEmployerHomePage = router.pathname.includes("/employer");

  if (isEmployerHomePage) {
    return (
      <>
        <NotLoggedInNavLinkButton
          label="Post Job"
          onClick={() =>
            loginViaGoogleWithRedirectTarget({
              pathname: router.pathname,
              redirectUrl: `${getFrontendUrl()}/postJob`,
            })
          }
          isCurrentPage={router.pathname.includes("/postJob")}
        />
      </>
    );
  }

  return (
    <>
      <SecondaryButton
        label="Sign up"
        onClick={() => loginViaGoogle(router.pathname)}
      />
      <PrimaryButton
        label="Log in"
        onClick={() => loginViaGoogle(router.pathname)}
      />
      <a href={instagramLink} target="_blank">
        <InstagramIconButton />
      </a>
    </>
  );
};

export default NotLoggedInHeaderPanel;
