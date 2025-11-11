import { Text, TextProps } from "@mantine/core";

const SpacedOutText = ({ label, ...props }: { label: string } & TextProps) => {
  return (
    <Text
      tt="uppercase"
      c="white"
      fw="400"
      lh="1.17"
      maw={280}
      lts={4}
      fz={{ base: 16, sm: 18, xl: 18 }}
      mb={33}
      {...props}
    >
      {label}
    </Text>
  );
};
export default SpacedOutText;
