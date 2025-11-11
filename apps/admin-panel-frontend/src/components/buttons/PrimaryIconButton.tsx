import { ActionIcon, ActionIconProps } from "@mantine/core";
import React from "react";
import classes from "styles/actionIcon.module.css";
const PrimaryIconButton = ({
  children,
  onClick,
  ...props
}: { children: React.ReactNode; onClick?: () => void } & ActionIconProps) => {
  return (
    <ActionIcon
      variant="filled"
      aria-label="Settings"
      bg="primaryOrange.5"
      className={classes.actionIcon}
      w={40}
      h={40}
      style={{ borderRadius: "100%" }}
      onClick={onClick}
      {...props}
    >
      {children}
    </ActionIcon>
  );
};
export default PrimaryIconButton;
