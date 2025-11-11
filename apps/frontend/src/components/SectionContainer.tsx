import { MantineStyleProp, Paper, PaperProps } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";

const SectionContainer = ({
  children,
  style,
  ...props
}: { children: React.ReactNode; style?: MantineStyleProp } & PaperProps) => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  return (
    <Paper
      style={{
        overflowY: "auto",
        borderRadius: isMobile ? 10 : 38,
        border: "1px solid",
        borderColor: "var(--mantine-color-primaryGreen-3)",
        boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)",
        ...style,
      }}
      h="70vh"
      maw="100%"
      bg="primaryDarkBlue.9"
      px={{ base: 20, sm: 50, xl: 70 }}
      py={{ base: 20, sm: 50, xl: 60 }}
      pos="relative"
      {...props}
    >
      {children}
    </Paper>
  );
};
export default SectionContainer;
