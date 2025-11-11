import { ButtonProps } from "@mantine/core";
import CustomSecondaryButton from "@/components/buttons/CustomSecondaryButton";
import { PolymorphicComponentProps } from "@mantine/core/lib/core/factory/create-polymorphic-component";
const SecondaryButton = ({
  label,
  onClick,
  ...props
}: {
  label: string;
  onClick?: () => void;
  } & PolymorphicComponentProps<"button", ButtonProps>) => {
  
  return (
    <CustomSecondaryButton
      styles={{
        root: {
          border: "1px solid",
          borderColor: "var(--mantine-color-primarySkyBlue-6)",
        },
      }}
      label={label}
      onClick={onClick}
      {...props}
    />
  );
};

export default SecondaryButton;
