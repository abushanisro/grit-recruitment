import CustomModal from "@/components/CustomModal";
import { useMediaQuery } from "@mantine/hooks";
import { getFrontendUrl } from "@/env";
import UnPromotedComponent from "./PostPaidCommonComponents/UnPromotedComponent";

const JobFormSubscribeModal = ({
  opened,
  onClose,
}: {
  opened: boolean;
  onClose: () => void;
}) => {
  const isMobile = useMediaQuery("(max-width: 525px)");

  const handleClick = () => {
    window.open(getFrontendUrl() + "/postPaidJobPlans", "_blank");
    onClose();
  };
  return (
    <CustomModal
      styles={{
        content: {
          background: "var(--mantine-color-secondaryBlue-9",
          border: "1px solid var(--mantine-color-secondarySkyBlue-4)",
          borderRadius: isMobile ? 10 : 30,
        },
      }}
      opened={opened}
      closeOnClickOutside={true}
      onClose={onClose}
    >
      <UnPromotedComponent handleClick={handleClick} />
    </CustomModal>
  );
};

export default JobFormSubscribeModal;
