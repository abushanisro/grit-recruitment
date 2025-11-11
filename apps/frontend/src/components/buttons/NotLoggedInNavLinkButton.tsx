import { ButtonProps } from "@mantine/core";
import { PolymorphicComponentProps } from "@mantine/core/lib/core/factory/create-polymorphic-component";
import CustomButton from "@/components/buttons/CustomButton";
import { useState } from "react";

const NotLoggedInNavLinkButton = ({
  label,
  isCurrentPage,
  ...props
}: {
  label: string;
  isCurrentPage: boolean;
} & PolymorphicComponentProps<"button", ButtonProps>) => {
  const [isHovered, setIsHovered] = useState<boolean>(false);

  return (
    <CustomButton
      label={label}
      c={isHovered ? "black" : isCurrentPage ? "black" : "primarySkyBlue.6"}
      bg={
        isHovered && !isCurrentPage
          ? "primarySkyBlue.6"
          : isHovered && isCurrentPage
          ? "primarySkyBlue.8"
          : isCurrentPage
          ? "primarySkyBlue.6"
          : "none"
      }
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      styles={{
        root: {
          border: "1px solid",
          borderColor: "var(--mantine-color-primarySkyBlue.6)",
        },
      }}
      {...props}
    />
  );
};
export default NotLoggedInNavLinkButton;
