import { ButtonProps } from "@mantine/core";
import { PolymorphicComponentProps } from "@mantine/core/lib/core/factory/create-polymorphic-component";
import { MouseEventHandler } from "react";
import CustomButton from "@components/buttons//CustomButton";

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
