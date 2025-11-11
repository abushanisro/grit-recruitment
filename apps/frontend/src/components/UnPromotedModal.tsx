import CustomModal from "@/components/CustomModal";

import { useMediaQuery } from "@mantine/hooks";

import { useRouter } from "next/router";
import UnPromotedComponent from "./PostPaidCommonComponents/UnPromotedComponent";

const UnPromotedModal = ({
  opened,
  onClose,
}: {
  opened: boolean;
  onClose: () => void;
}) => {
  const isMobile = useMediaQuery("(max-width: 525px)");
  const router = useRouter();
  const handleClick = () => {
    router.push("/postPaidPlans");
  };
  return (
    <CustomModal
      styles={{
        content: {
          background: "black",

          border: "1px solid var(--mantine-color-secondaryGreen-1)",
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

export default UnPromotedModal;
