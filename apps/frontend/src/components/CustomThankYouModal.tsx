import { Modal, ModalProps } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";

const CustomThankYouModal = ({
  opened,
  onClose,
  children,
  ...props
}: {
  opened: boolean;
  onClose: () => void;
  children: React.ReactNode;
} & ModalProps) => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  return (
    <Modal
      opened={opened}
      onClose={onClose}
      centered
      closeOnClickOutside={false}
      closeOnEscape={false}
      withCloseButton={false}
      size={540}
      styles={{
        content: {
          paddingTop: 10,
          paddingBottom: 20,
          paddingInline: 14,
          background: "black",
          overflowY: "auto",

          borderRadius: isMobile ? 10 : 30,
        },
      }}
      {...props}
    >
      {children}
    </Modal>
  );
};

export default CustomThankYouModal;
