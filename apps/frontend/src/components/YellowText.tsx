import { Text, TextProps } from "@mantine/core";

export const YellowText = ({
  label,
  ...props
}: { label: string } & TextProps) => {
  return (
    <Text
      fz={{ base: 16, sm: 20, xl: 24 }}
      fw={400}
      lh="1.17"
      lineClamp={2}
      c="secondaryGreen.1"
      style={{ wordBreak: "break-word" }}
      {...props}
    >
      {label}
    </Text>
  );
};
export default YellowText;
