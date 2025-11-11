import { Button, ButtonProps } from "@mantine/core";
import { PolymorphicComponentProps } from "@mantine/core/lib/core/factory/create-polymorphic-component";
import { MouseEventHandler } from "react";
import { useHover } from "@mantine/hooks";
const CustomButton = ({
  label,
  onClick,
  ...props
}: {
  label?: string | React.ReactNode;
  onClick?: MouseEventHandler<HTMLButtonElement> | undefined;
} & PolymorphicComponentProps<"button", ButtonProps>) => {
  const { hovered, ref } = useHover<HTMLButtonElement>();
  return (
    <Button
      ref={ref}
      radius={10}
      px={{ base: 10, xl: 20 }}
      py={5}
      h={40}
      onClick={onClick}
      fz={{ base: 14, sm: 16, xl: 20 }}
      lh={{ md: "1.2", lg: "1.17" }}
      c="secondaryDarkBlue.9"
      fw={{ base: "400", sm: "600" }}
      bg={hovered ? "primarySkyBlue.8" : "primarySkyBlue.6"}
      {...props}
    >
      {label}
    </Button>
  );
};

export default CustomButton;
