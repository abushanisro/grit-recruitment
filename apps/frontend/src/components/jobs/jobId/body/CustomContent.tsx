import { Text, TextProps } from "@mantine/core";

const CustomContent = ({ label, ...props }: { label: string } & TextProps) => {
  return (
    <Text
      fw={400}
      fz={{ base: 16, xl: 22 }}
      lh="1.3"
      c="white"
      style={{ wordBreak: "break-word" }}
      {...props}
    >
      {label}
    </Text>
  );
};
export default CustomContent;
