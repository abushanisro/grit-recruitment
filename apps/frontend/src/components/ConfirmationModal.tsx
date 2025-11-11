import CustomModal from "@/components/CustomModal";
import PrimaryButton from "@/components/buttons/PrimaryButton";
import { Box, Title } from "@mantine/core";
import SecondaryButton from "@/components/buttons/SecondaryButton";
import classes from "styles/buttonHover.module.css";
const ConfirmationModal = ({
  opened,
  onClose,
  loading,
  modalHeader,
  onSuccessFn,
  secondaryButtonLabel,
  successButtonLabel,
}: {
  opened: boolean;
  onClose: () => void;
  loading?: boolean;
  modalHeader: string;
  onSuccessFn: () => void;
  successButtonLabel: string;
  secondaryButtonLabel?: string;
}) => {
  return (
    <CustomModal opened={opened} onClose={onClose}>
      <Title order={3} c="white" mb={40}>
        {modalHeader}
      </Title>
      <Box ta="right">
        <SecondaryButton
          styles={{
            root: {
              border: "1px solid",
              borderColor: "var(--mantine-color-primarySkyBlue-6)",
            },
          }}
          label={secondaryButtonLabel ?? "Cancel"}
          fz={{ base: 16, sm: 20 }}
          fw="600"
          px={20}
          py={12}
          w={{ base: "100%", xs: "max-content" }}
          h="max-content"
          onClick={onClose}
        />
        <PrimaryButton
          label={successButtonLabel}
          className={classes.primarySkyBlue}
          bg="primarySkyBlue.6"
          fz={{ base: 16, sm: 20 }}
          fw="600"
          px={20}
          py={12}
          w={{ base: "100%", xs: "max-content" }}
          ml={{ base: 0, xs: 20 }}
          mt={{ base: 10, xs: 0 }}
          h="max-content"
          loading={loading}
          onClick={onSuccessFn}
        />
      </Box>
    </CustomModal>
  );
};

export default ConfirmationModal;
