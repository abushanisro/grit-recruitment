import CustomButton from "@/components/buttons/CustomButton";
import { ButtonProps } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { useState } from "react";

export const ActiveTableTabButton = ({
  isActive = false,
  label,
  onClick,
  ...props
}: {
  isActive: boolean;
  label: string;
  onClick?: () => void;
} & ButtonProps) => {
  const [isHovered, setIsHovered] = useState<boolean>(false);
  const isTablet = useMediaQuery("(max-width: 768px)");
  return (
    <CustomButton
      onClick={onClick}
      radius={0}
      label={label}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      c={
        isActive && isHovered
          ? "black"
          : isActive
          ? "black"
          : isHovered
          ? "black"
          : "secondaryGreen.1"
      }
      bg={
        isActive && isHovered
          ? "primarySkyBlue.2"
          : isActive
          ? "secondaryGreen.1"
          : isHovered
          ? "secondaryGreen.1"
          : "transparent"
      }
      fw="700"
      styles={{
        root: {
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryGreen-1)",
          borderRadius: "10px 10px 0 0",
          borderBottomRightRadius: isTablet ? "10px" : "0",
          borderBottomLeftRadius: isTablet ? "10px" : "0",
        },
      }}
      {...props}
    />
  );
};
