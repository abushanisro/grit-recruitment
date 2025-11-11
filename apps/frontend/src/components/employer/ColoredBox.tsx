import { Flex } from "@mantine/core";

const ColoredBox = ({
  children,
  style,
}: {
  children: React.ReactNode;
  style?: Record<string, any>;
}) => {
  return (
    <Flex
      w="100%"
      p={21}
      style={{ borderRadius: 9.75, maxWidth: 245, ...style }}
    >
      {children}
    </Flex>
  );
};
export default ColoredBox;
