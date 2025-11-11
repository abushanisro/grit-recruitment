import { Paper, PaperProps } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";

const SectionTableContainer = ({
  children,
  ...props
}: {
  children: React.ReactNode;
} & PaperProps) => {
  const isMobile = useMediaQuery("(max-width: 426px)");
  return (
    <Paper
      style={{
        borderRadius: isMobile ? 10 : 28,
        border: "1px solid",
        borderColor: "var(--mantine-color-primaryGreen-3)",
        boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)",
      }}
      h="100%"
      w="100%"
      p={{ base: 20, md: 24, lg: 38 }}
      bg="transparent"
      mih="40vh"
      {...props}
    >
      {children}
    </Paper>
  );
};
export default SectionTableContainer;
