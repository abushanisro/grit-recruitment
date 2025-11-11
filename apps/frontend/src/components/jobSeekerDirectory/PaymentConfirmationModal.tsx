import CustomModal from "@/components/CustomModal";
import PrimaryButton from "@/components/buttons/PrimaryButton";
import { Box, Title } from "@mantine/core";
import SecondaryButton from "@/components/buttons/SecondaryButton";
import { useMediaQuery } from "@mantine/hooks";
import classes from "/styles/editButton.module.css";

const ConfirmationModal = ({
  opened,
  onClose,
  modalHeader,
  onSuccessFn,
  secondaryButtonLabel,
  successButtonLabel,
}: {
  opened: boolean;
  onClose: () => void;
  modalHeader: string;
  onSuccessFn: () => void;
  successButtonLabel: string;
  secondaryButtonLabel?: string;
}) => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  return (
    <CustomModal
      styles={{
        content: {
          background: "black",
          padding: 20,
          border: "1px solid var(--mantine-color-secondarySkyBlue-4)",
          borderRadius: isMobile ? 10 : 30,
        },
      }}
      opened={opened}
      onClose={onClose}
    >
      <Title order={3} c="white" mb={40}>
        {modalHeader}
      </Title>
      <Box ta="right">
        <SecondaryButton
          bg="black"
          styles={{
            root: {
              border: "1px solid",
              borderColor: "var(--mantine-color-primarySkyBlue-6)",
            },
          }}
          c="primarySkyBlue.6"
          label={secondaryButtonLabel ?? "Cancel"}
          fz={{ base: 16, sm: 20 }}
          fw="600"
          px={20}
          py={12}
          className={classes.DownloadButton}
          w={{ base: "100%", xs: "max-content" }}
          h="max-content"
          onClick={onClose}
        />
        <PrimaryButton
          label={successButtonLabel}
          bg="primarySkyBlue.6"
          fz={{ base: 16, sm: 20 }}
          fw="600"
          px={20}
          py={12}
          w={{ base: "100%", xs: "max-content" }}
          ml={{ base: 0, xs: 20 }}
          mt={{ base: 10, xs: 0 }}
          h="max-content"
          className={classes.GoButton}
          onClick={onSuccessFn}
        />
      </Box>
    </CustomModal>
  );
};

export default ConfirmationModal;
