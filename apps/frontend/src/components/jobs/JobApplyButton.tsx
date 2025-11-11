import React from "react";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { PolymorphicComponentProps } from "@mantine/core/lib/core/factory/create-polymorphic-component";
import { ButtonProps } from "@mantine/core";
import classes from "styles/jobApplyButton.module.css";
const JobApplyButton = ({
  label,
  ...props
}: { label: string } & PolymorphicComponentProps<"button", ButtonProps>) => {
  return (
    <PrimaryButton
      pos={{ base: "fixed", xs: "relative" }}
      className={classes.applyButton}
      bottom={0}
      left={0}
      right={0}
      label={label}
      h={{ base: 40, md: 40, xl: 50 }}
      px={{ base: 12, md: 12, lg: 16 }}
      fz={{ base: 16, sm: 20, xl: 24 }}
      mx={{ base: 10, xs: 0 }}
      mb={{ base: 10, xs: 0 }}
      fw={600}
      bg="primarySkyBlue.6"
      c="black"
      {...props}
    />
  );
};

export default JobApplyButton;
