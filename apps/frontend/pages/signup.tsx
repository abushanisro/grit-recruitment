import { Image, Flex, Group, Text } from "@mantine/core";
import { useRouter } from "next/router";
import Head from "next/head";
import { getQueryClient } from "api";
import CustomSkeleton from "@components/CustomSkeleton";
import SignUpFormPage from "@components/signup/SignUpForm";
import PageLayout from "@components/layouts/PageLayout";
import { useEffect } from "react";
import PrimaryButton from "@components/buttons/PrimaryButton";
import Link from "next/link";
import { contract } from "contract";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { getErrorMessage } from "@components/ErrorMessage";

const signUpToastId = "signUpToastId";

const CustomHeader = () => {
  return (
    <Group
      h="100%"
      justify="space-between"
      w={{ base: "80%", sm: "100%" }}
      align="center"
    >
      <Link href="/">
        <Image
          src="/images/mainLogo.png"
          alt="logo"
          w={{ base: 200, sm: 260, xl: 300 }}
          h={{ base: 20, sm: 23 }}
        />
      </Link>
    </Group>
  );
};

const SignUp = () => {
  const { showToast } = useCustomToast();
  const router = useRouter();
  const refId = String(router.query.refId ?? "");
  const { data, isLoading, error } =
    getQueryClient().auth.getGoogleUser.useQuery(
      [contract.auth.getGoogleUser.path, refId],
      {
        query: { refId: refId },
      }
    );

  useEffect(() => {
    if (!data && error) {
      showToast({
        status: ToastStatus.error,
        id: signUpToastId,
        message: "Unable to fetch google data. Please try again",
      });
      router.push("/");
    }
  }, [error]);

  if (isLoading) {
    return (
      <Flex
        h="100vh"
        style={{ flexDirection: "column" }}
        justify="center"
        align="center"
        gap={10}
        bg="customBlack.4"
      >
        <CustomSkeleton
          height={400}
          radius="xl"
          mx="auto"
          maw={{ base: 1037, xl: 1037 }}
        />
      </Flex>
    );
  }
  if (error) {
    return (
      <Flex
        h="100vh"
        style={{ flexDirection: "column" }}
        justify="center"
        align="center"
        gap={10}
        bg="customBlack.4"
      >
        <Text c="white">{getErrorMessage(error)}</Text>
        <Link href="/">
          <PrimaryButton label="Go to homepage" />
        </Link>
      </Flex>
    );
  }

  return (
    <>
      <>
        <Head>
          <title>Sales Jobverse | Sign up</title>
          <meta name="description" content="Sales Jobverse Sign up" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <link rel="icon" href="/favicon.ico" />
        </Head>
      </>
      <PageLayout
        headerComponent={<CustomHeader />}
        mainComponent={<SignUpFormPage data={data.body} />}
      />
    </>
  );
};

export default SignUp;
