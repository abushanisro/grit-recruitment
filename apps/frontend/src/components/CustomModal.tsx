import { Modal, ModalProps } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";

const CustomModal = ({
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
      overlayProps={{
        backgroundOpacity: 0.55,
        blur: 3,
      }}
      centered
      closeOnClickOutside={false}
      closeOnEscape={false}
      withCloseButton={false}
      size={"auto"}
      styles={{
        content: {
          padding: 20,
          background: "var(--mantine-color-primaryDarkBlue-9)",
          border: "1px solid var(--mantine-color-primarySkyBlue-6)",
          borderRadius: isMobile ? 10 : 30,
        },
      }}
      {...props}
    >
      {children}
    </Modal>
  );
};

export default CustomModal;
