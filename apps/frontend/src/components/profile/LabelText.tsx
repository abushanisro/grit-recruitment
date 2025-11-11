import { Text, TextProps } from "@mantine/core";
import { RegisterOptions } from "react-hook-form";
const LabelText = ({
  label,
  rules,
  ...props
}: { label: string; rules?: RegisterOptions } & TextProps) => {
  return (
    <Text c="secondaryGreen.1" fw={700} fz={17} pb={10} {...props}>
      {label}{" "}
      {rules?.required ? (
        <Text span c="red">
          *
        </Text>
      ) : (
        ""
      )}
    </Text>
  );
};
export default LabelText;
