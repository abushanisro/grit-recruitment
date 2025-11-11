import React from "react";
import { Text, TextProps } from "@mantine/core";
const CustomHeadingText = ({
  label,
  ...props
}: { label: string } & TextProps) => {
  return (
    <Text fw={700} c={"secondaryGreen.1"} fz={{ base: 20, md: 28 }} {...props}>
      {label}
    </Text>
  );
};

export default CustomHeadingText;
