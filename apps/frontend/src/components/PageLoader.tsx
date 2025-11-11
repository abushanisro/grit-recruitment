import { Loader, Modal, Center } from "@mantine/core";

interface PageLoaderProps {
  opened: boolean;
  onClose: () => void;
}

const PageLoader = (props: PageLoaderProps) => {
  return (
    <Modal
      opened={props.opened}
      onClose={props.onClose}
      w="100%"
      h="100%"
      overlayProps={{
        backgroundOpacity: 0.55,
        blur: 3,
      }}
      centered
      closeOnClickOutside={false}
      closeOnEscape={false}
      withCloseButton={false}
      bg="transparent"
      styles={{
        content: { background: "transparent" },
      }}
    >
      <Center>
        <Loader
          color="primarySkyBlue.6"
          size="xl"
          type="bars"
          bg="transparent"
        />
      </Center>
    </Modal>
  );
};

export default PageLoader;
