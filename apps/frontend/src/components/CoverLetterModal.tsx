import CustomModal from "@/components/CustomModal";
import PrimaryButton from "@/components/buttons/PrimaryButton";
import { Box, Title, Text } from "@mantine/core";

const CoverLetterModal = ({
  opened,
  onClose,
  modalHeader,
  coverLetter,
}: {
  opened: boolean;
  onClose: () => void;
  modalHeader: string;
  coverLetter: string;
}) => {
  return (
    <CustomModal
      opened={opened}
      onClose={onClose}
      size="xl"
      closeOnClickOutside={true}
      closeOnEscape={true}
    >
      <Title order={3} c="white" mb={20}>
        {modalHeader}
      </Title>
      <Text c="white" style={{ wordBreak: "break-word" }}>
        {coverLetter}
      </Text>
      <Box ta="right" mt={10}>
        <PrimaryButton
          label="Okay"
          fz={{ base: 16, sm: 20 }}
          fw="600"
          px={20}
          py={12}
          w={{ base: "100%", xs: "max-content" }}
          ml={{ base: 0, xs: 20 }}
          mt={{ base: 10, xs: 0 }}
          h="max-content"
          onClick={onClose}
        />
      </Box>
    </CustomModal>
  );
};

export default CoverLetterModal;
