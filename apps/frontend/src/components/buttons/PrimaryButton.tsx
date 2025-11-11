import { ButtonProps } from "@mantine/core";
import CustomButton from "@/components/buttons/CustomButton";
import { PolymorphicComponentProps } from "@mantine/core/lib/core/factory/create-polymorphic-component";
import { MouseEventHandler } from "react";

const PrimaryButton = ({
  label,
  onClick,
  ...props
}: {
  label: string;
  onClick?: MouseEventHandler<HTMLButtonElement> | undefined;
} & PolymorphicComponentProps<"button", ButtonProps>) => {
  return <CustomButton label={label} onClick={onClick} {...props} />;
};

export default PrimaryButton;
