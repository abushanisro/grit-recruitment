import { Paper } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";

const CustomContainer = ({ children }: { children: React.ReactNode }) => {
  const isMobile = useMediaQuery("(max-width: 426px)");
  return (
    <Paper
      style={{
        overflowY: "auto",
        borderRadius: isMobile ? 10 : 38,
        border: "1px solid",
        borderColor: "var(--mantine-color-secondaryGreen-1)",
        boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)",
      }}
      h="100%"
      maw="100%"
      bg=" linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
      mih="70vh"
    >
      {children}
    </Paper>
  );
};
export default CustomContainer