import { Stack, Text, Group } from "@mantine/core";

import { useDisclosure, useMediaQuery } from "@mantine/hooks";
import { useHover } from "@mantine/hooks";

import CustomSkeleton from "@components/CustomSkeleton";

import { useRouter } from "next/router";
import PaymentConfirmationModal from "./PaymentConfirmationModal";
import { useLogin } from "@/contexts/LoginProvider";
import { loginViaGoogleWithRedirectTarget } from "@/utils/common";
import { getFrontendUrl } from "@/env";

const JobSeekerDummyCard = () => {
  const isMobile = useMediaQuery("(max-width: 425px)");

  const { hovered, ref } = useHover();

  const [opened, { open, close }] = useDisclosure(false);

  const router = useRouter();
  const { isLoggedIn } = useLogin();

  const handleClick = () => {
    router.push("/subscription");
    close();
  };

  const handleLogin = () => {
    if (!isLoggedIn) {
      loginViaGoogleWithRedirectTarget({
        pathname: router.pathname,
        redirectUrl: `${getFrontendUrl()}/jobseekerDirectory`,
      });
    } else {
      open();
    }
  };
  return (
    <>
      <PaymentConfirmationModal
        opened={opened}
        onClose={close}
        modalHeader={`Please subscribe to our plans to view this page`}
        onSuccessFn={handleClick}
        successButtonLabel="View Plans"
        secondaryButtonLabel="Cancel"
      />
      <Stack
        ref={ref}
        pos="relative"
        bg={hovered ? "#101010" : "black"}
        w="100%"
        onClick={handleLogin}
        px={{ base: 40, sm: 80 }}
        py={{ base: 30, sm: 30 }}
        maw={1100}
        mt={30}
        h="100%"
        style={{
          borderRadius: isMobile ? 10 : 38,
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryGreen-1)",
          boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)",
          cursor: "pointer",
        }}
      >
        <Group gap={isMobile ? 10 : "40"}>
          <CustomSkeleton height={90} circle mb="sm" />
          <Stack>
            <Text
              c="white"
              fz={{ base: 8, sm: 20 }}
              style={{
                filter: "blur(4px)",
              }}
            >
              JOHN DOE
            </Text>
            <Text
              c="white"
              fz={{ base: 12, sm: 20 }}
              style={{
                filter: "blur(4px)",
              }}
            >
              FULL STACK DEVELOPER
            </Text>
          </Stack>
        </Group>
        <Text
          style={{
            filter: "blur(4px)",
          }}
          fz={{ base: 8, sm: 16 }}
        >
          Lorem ipsum dolor sit amet consectetur adipisicing elit. Dolore,
          maxime!
        </Text>
        <Text
          style={{
            filter: "blur(4px)",
          }}
          fz={{ base: 8, sm: 16 }}
        >
          Lorem ipsum dolor sit amet consectetur adipisicing elit. Dolore,
          maxime!
        </Text>
      </Stack>
      <Stack
        ref={ref}
        pos="relative"
        bg={hovered ? "#101010" : "black"}
        w="100%"
        onClick={handleLogin}
        px={{ base: 40, sm: 80 }}
        py={{ base: 30, sm: 30 }}
        maw={1100}
        mt={30}
        h="100%"
        style={{
          borderRadius: isMobile ? 10 : 38,
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryGreen-1)",
          boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)",
          cursor: "pointer",
        }}
      >
        <Group gap={isMobile ? 10 : "40"}>
          <CustomSkeleton height={90} circle mb="sm" />
          <Stack>
            <Text
              c="white"
              fz={{ base: 8, sm: 20 }}
              style={{
                filter: "blur(4px)",
              }}
            >
              JOHN DOE
            </Text>
            <Text
              c="white"
              fz={{ base: 12, sm: 20 }}
              style={{
                filter: "blur(4px)",
              }}
            >
              FULL STACK DEVELOPER
            </Text>
          </Stack>
        </Group>
        <Text
          style={{
            filter: "blur(4px)",
          }}
          fz={{ base: 8, sm: 16 }}
        >
          Lorem ipsum dolor sit amet consectetur adipisicing elit. Dolore,
          maxime!
        </Text>
        <Text
          style={{
            filter: "blur(4px)",
          }}
          fz={{ base: 8, sm: 16 }}
        >
          Lorem ipsum dolor sit amet consectetur adipisicing elit. Dolore,
          maxime!
        </Text>
      </Stack>
      <Stack
        ref={ref}
        pos="relative"
        bg={hovered ? "#101010" : "black"}
        w="100%"
        onClick={handleLogin}
        px={{ base: 40, sm: 80 }}
        py={{ base: 30, sm: 30 }}
        maw={1100}
        mt={30}
        h="100%"
        style={{
          borderRadius: isMobile ? 10 : 38,
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryGreen-1)",
          boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)",
          cursor: "pointer",
        }}
      >
        <Group gap={isMobile ? 10 : "40"}>
          <CustomSkeleton height={90} circle mb="sm" />
          <Stack>
            <Text
              c="white"
              fz={{ base: 8, sm: 20 }}
              style={{
                filter: "blur(4px)",
              }}
            >
              JOHN DOE
            </Text>
            <Text
              c="white"
              fz={{ base: 12, sm: 20 }}
              style={{
                filter: "blur(4px)",
              }}
            >
              FULL STACK DEVELOPER
            </Text>
          </Stack>
        </Group>
        <Text
          style={{
            filter: "blur(4px)",
          }}
          fz={{ base: 8, sm: 16 }}
        >
          Lorem ipsum dolor sit amet consectetur adipisicing elit. Dolore,
          maxime!
        </Text>
        <Text
          style={{
            filter: "blur(4px)",
          }}
          fz={{ base: 8, sm: 16 }}
        >
          Lorem ipsum dolor sit amet consectetur adipisicing elit. Dolore,
          maxime!
        </Text>
      </Stack>
      <Stack
        ref={ref}
        pos="relative"
        bg={hovered ? "#101010" : "black"}
        w="100%"
        onClick={handleLogin}
        px={{ base: 40, sm: 80 }}
        py={{ base: 30, sm: 30 }}
        maw={1100}
        mt={30}
        h="100%"
        style={{
          borderRadius: isMobile ? 10 : 38,
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryGreen-1)",
          boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)",
          cursor: "pointer",
        }}
      >
        <Group gap={isMobile ? 10 : "40"}>
          <CustomSkeleton height={90} circle mb="sm" />
          <Stack>
            <Text
              c="white"
              fz={{ base: 8, sm: 20 }}
              style={{
                filter: "blur(4px)",
              }}
            >
              JOHN DOE
            </Text>
            <Text
              c="white"
              fz={{ base: 12, sm: 20 }}
              style={{
                filter: "blur(4px)",
              }}
            >
              FULL STACK DEVELOPER
            </Text>
          </Stack>
        </Group>
        <Text
          style={{
            filter: "blur(4px)",
          }}
          fz={{ base: 8, sm: 16 }}
        >
          Lorem ipsum dolor sit amet consectetur adipisicing elit. Dolore,
          maxime!
        </Text>
        <Text
          style={{
            filter: "blur(4px)",
          }}
          fz={{ base: 8, sm: 16 }}
        >
          Lorem ipsum dolor sit amet consectetur adipisicing elit. Dolore,
          maxime!
        </Text>
      </Stack>
      <Stack
        ref={ref}
        pos="relative"
        bg={hovered ? "#101010" : "black"}
        w="100%"
        onClick={handleLogin}
        px={{ base: 40, sm: 80 }}
        py={{ base: 30, sm: 30 }}
        maw={1100}
        mt={30}
        h="100%"
        style={{
          borderRadius: isMobile ? 10 : 38,
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryGreen-1)",
          boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)",
          cursor: "pointer",
        }}
      >
        <Group gap={isMobile ? 10 : "40"}>
          <CustomSkeleton height={90} circle mb="sm" />
          <Stack>
            <Text
              c="white"
              fz={{ base: 8, sm: 20 }}
              style={{
                filter: "blur(4px)",
              }}
            >
              JOHN DOE
            </Text>
            <Text
              c="white"
              fz={{ base: 12, sm: 20 }}
              style={{
                filter: "blur(4px)",
              }}
            >
              FULL STACK DEVELOPER
            </Text>
          </Stack>
        </Group>
        <Text
          style={{
            filter: "blur(4px)",
          }}
          fz={{ base: 8, sm: 16 }}
        >
          Lorem ipsum dolor sit amet consectetur adipisicing elit. Dolore,
          maxime!
        </Text>
        <Text
          style={{
            filter: "blur(4px)",
          }}
          fz={{ base: 8, sm: 16 }}
        >
          Lorem ipsum dolor sit amet consectetur adipisicing elit. Dolore,
          maxime!
        </Text>
      </Stack>
    </>
  );
};
export default JobSeekerDummyCard;
