import { Button, SimpleGrid } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { IconDownload } from "@tabler/icons-react";
import React from "react";
import classes from "styles/editButton.module.css";

const ThankYouButtonComponent = ({
  invoiceLink,
  goToButton,
}: {
  invoiceLink: string;
  goToButton: React.ReactNode;
}) => {
  const isMobile = useMediaQuery("(max-width: 426px)");
  return (
    <div>
      <SimpleGrid mt={10} cols={{ base: 1, sm: 2 }}>
        <Button
          component="a"
          href={invoiceLink}
          className={classes.DownloadButton}
          target="blank"
          bg="transparent"
          style={{
            border: "1px solid var(--mantine-color-primarySkyBlue-6)",
            borderRadius: "8px",
          }}
          leftSection={<IconDownload size={isMobile ? 14 : 20} />}
          h={44}
          fw={600}
          py={10}
          fz={{ base: 16, md: 18 }}
          c="#FFFFFFB8"
        >
          Download Invoice
        </Button>
        {goToButton}
      </SimpleGrid>
    </div>
  );
};

export default ThankYouButtonComponent;
