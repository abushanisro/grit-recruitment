import { Box, BoxProps } from "@mantine/core";

const BubbleLinkButton = ({
  onMouseLeave,
  onMouseEnter,
  label,
  handleClick,
  ...props
}: {
  label: string;
  onMouseLeave?: () => void;
  onMouseEnter?: () => void;
  handleClick?: () => void;
} & BoxProps) => {
  return (
    <Box
      c="black"
      fz={{ base: 16, md: 18, lg: 20, xl: 24 }}
      fw="700"
      lh="1.17"
      onMouseLeave={onMouseLeave}
      onMouseEnter={onMouseEnter}
      onClick={() => (handleClick ? handleClick() : undefined)}
      px={43}
      py={21}
      w="100%"
      maw={{ md: 180, lg: 210 }}
      ta="center"
      style={{ borderRadius: 10, cursor: "pointer" }}
      {...props}
    >
      {label}
    </Box>
  );
};
export default BubbleLinkButton;
