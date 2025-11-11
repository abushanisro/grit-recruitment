import CustomButton from "@/components/buttons/CustomButton";
import { ButtonProps } from "@mantine/core";

export const ActiveTabButton = ({
  isActive = false,
  label,
  onClick,
  ...props
}: {
  isActive: boolean;
  label: string;
  onClick?: () => void;
} & ButtonProps) => {
  return (
    <CustomButton
      onClick={onClick}
      radius={0}
      label={label}
      c={isActive ? "primaryDarkBlue.9" : "secondaryGreen.1"}
      bg={isActive ? "secondaryGreen.1" : "transparent"}
      fw="700"
      styles={{
        root: {
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryGreen-1)",
          borderTopLeftRadius: "10px",
          borderTopRightRadius: "10px",
          borderBottomLeftRadius: "0px",
          borderBottomRightRadius: "0px",
        },
      }}
      {...props}
    />
  );
};
