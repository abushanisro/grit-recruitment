import { ButtonProps } from "@mantine/core";
import { PolymorphicComponentProps } from "@mantine/core/lib/core/factory/create-polymorphic-component";
import Link from "next/link";
import CustomButton from "@/components/buttons/CustomButton";
import { useState } from "react";

const NavLinkButton = ({
  label,
  pageLink,
  isCurrentPage,
  ...props
}: {
  label: string | React.ReactNode;
  pageLink: string;
  isCurrentPage: boolean;
} & PolymorphicComponentProps<"button", ButtonProps>) => {
  const [isHovered, setIsHovered] = useState<boolean>(false);
  return (
    <Link href={pageLink}>
      <CustomButton
        label={label}
        c={isHovered ? "black" : isCurrentPage ? "black" : "primarySkyBlue.6"}
        bg={
          isHovered
            ? "primarySkyBlue.6"
            : isCurrentPage
            ? "primarySkyBlue.6"
            : "none"
        }
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
        styles={{
          root: {
            border: "1px solid",
            borderColor: "var(--mantine-color-primarySkyBlue-6)",
            borderRadius: "6px",
          },
        }}
        {...props}
      />
    </Link>
  );
};
export default NavLinkButton;
