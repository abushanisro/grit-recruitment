import { Text, TextProps } from "@mantine/core";

const SubHeading = ({ label, ...props }: { label: string } & TextProps) => {
  return (
    <Text fw={700} fz={{ base: 20, sm: 24 }} lh="1.17" c="white" {...props}>
      {label}
    </Text>
  );
};

export default SubHeading;
