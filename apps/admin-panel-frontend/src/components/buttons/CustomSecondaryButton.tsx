import { Button, ButtonProps } from "@mantine/core";
import { PolymorphicComponentProps } from "@mantine/core/lib/core/factory/create-polymorphic-component";
import { MouseEventHandler } from "react";
import { useHover } from "@mantine/hooks";
const CustomSecondaryButton = ({
  label,
  onClick,
  ...props
}: {
  label?: string;
  onClick?: MouseEventHandler<HTMLButtonElement> | undefined;
} & PolymorphicComponentProps<"button", ButtonProps>) => {
  const { hovered, ref } = useHover<HTMLButtonElement>();
  return (
    <Button
      ref={ref}
      radius={10}
      px={{ base: 10, xl: 20 }}
      py={6}
      h={40}
      onClick={onClick}
      fz={{ base: 16, xl: 20 }}
      lh={{ md: "1.2", lg: "1.17" }}
      c={hovered ? "black" : "secondaryYellow.3"}
      fw="500"
      bg={hovered ? "secondaryYellow.3" : "black"}
      style={{ border: "1px solid", borderColor: "secondaryYellow.3" }}
      {...props}
    >
      {label}
    </Button>
  );
};

export default CustomSecondaryButton;
