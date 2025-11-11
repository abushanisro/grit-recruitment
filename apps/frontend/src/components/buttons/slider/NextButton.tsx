import { ActionIcon, ActionIconProps, Image } from "@mantine/core";
import { PolymorphicComponentProps } from "@mantine/core/lib/core/factory/create-polymorphic-component";

const NextButton = ({
  onClick,
  ...props
}: {
  onClick: (e: React.MouseEvent<HTMLButtonElement>) => void;
} & PolymorphicComponentProps<"button", ActionIconProps>) => {
  return (
    <ActionIcon
      variant="transparent"
      aria-label="next"
      pos="absolute"
      top="40%"
      right={10}
      c="primaryOrange.5"
      bg="secondaryRed.9"
      onClick={onClick}
      style={{ borderRadius: "100%" }}
      {...props}
    >
      <Image src="/images/nextButton.svg" w={32} h={32} alt="next-button" />
    </ActionIcon>
  );
};
export default NextButton;
