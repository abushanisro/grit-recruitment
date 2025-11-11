import { useRouter } from "next/router";
import { loginViaGoogle } from "@/utils/common";
import PrimaryButton from "@/components/buttons/PrimaryButton";

const NotLoggedInHeaderPanel = () => {
  const router = useRouter();

  return (
    <PrimaryButton
      label="Log in"
      onClick={() => loginViaGoogle(router.pathname)}
    />
  );
};

export default NotLoggedInHeaderPanel;
