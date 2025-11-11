import { loginViaGoogle } from "@/utils/common";
import { useRouter } from "next/router";
import CustomButton from "@/components/buttons/CustomButton";

const NotLoggedInNavbar = () => {
  const router = useRouter();
  return (
    <CustomButton
      label="Login"
      c="secondaryYellow.3"
      bg="black"
      styles={{
        root: {
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryYellow-3)",
        },
      }}
      w="100%"
      onClick={() => loginViaGoogle(router.pathname)}
    />
  );
};
export default NotLoggedInNavbar;
