import React from "react";
import { useRouter } from "next/router";
import { Button, ButtonProps } from "@mantine/core";
import { IconArrowLeft } from "@tabler/icons-react";

interface BackButtonProps extends ButtonProps {
  label: string;
  pathname: string;
  query?: Record<string, any>;
}

export const BackButton: React.FC<BackButtonProps> = ({
  label,
  pathname,
  query = {},
  ...props
}) => {
  const router = useRouter();

  const handleButtonClick = () => {
    router.push({
      pathname,
      query: { ...query },
    });
  };

  return (
    <Button
      onClick={() => handleButtonClick()}
      c="primarySkyBlue.6"
      fw={400}
      fz={{ base: 20 }}
      mb={15}
      bg="transparent"
      px={0}
      justify="flex-start"
      {...props}
    >
      <IconArrowLeft stroke={2} /> {label}
    </Button>
  );
};
