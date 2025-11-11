import { useRouter } from "next/router";
import { createContext, useContext } from "react";
import { useLogin } from "@/contexts/LoginProvider";
import { getQueryClient } from "api";
import { UserProfileType } from "@/types/profile";
import { contract } from "contract";
import ErrorMessage from "@components/ErrorMessage";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import { Center } from "@mantine/core";
import ProfileLoader from "@components/loaders/ProfileLoader";

interface ContextInterface {
  userDetails: UserProfileType | null;
}

const UserDataContext = createContext<ContextInterface>({
  userDetails: null,
});

const UserProvider = ({ children }: { children: React.ReactNode }) => {
  const { data, isLoading, error } =
    getQueryClient().user.getUserProfile.useQuery(
      [contract.user.getUserProfile.path],
      {}
    );
  if (isLoading) {
    return (
      <Center bg="customBlack.4" h="100vh">
        <ProfileLoader />
      </Center>
    );
  }
  if (error) {
    return <ErrorMessage error={error} />;
  }
  if (data?.status !== 200) {
    return <CustomErrorMessage errorMessage="User data not available" />;
  }
  return (
    <UserDataContext.Provider
      value={{
        userDetails: data.body ?? null,
      }}
    >
      {children}
    </UserDataContext.Provider>
  );
};
export const UserDataProvider = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const router = useRouter();
  const { isLoggedIn } = useLogin();

  if (
    !isLoggedIn ||
    router.asPath.includes("signup") ||
    router.asPath.includes("login")
  ) {
    return <> {children}</>;
  }
  return <UserProvider>{children}</UserProvider>;
};

export const useUserData = () => useContext(UserDataContext);
