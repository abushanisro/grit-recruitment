import CustomModal from "@/components/CustomModal";

import { Box, Flex, Stack } from "@mantine/core";

import { useMediaQuery } from "@mantine/hooks";

import CustomPostBoostCard from "@components/CustomPostBoostCard";
import { Dispatch, SetStateAction, useState } from "react";

import { PostPaidActiveSubscriptionResponseType } from "@/types/postPaid";

import { UseFormReturn } from "react-hook-form";
import { ModifiedCreateJobType } from "@/types/jobs";
import PostPaidModalHeader from "@components/PostPaidCommonComponents/postPaidModalHeader";
import PostPaidModalTextComponent from "@components/PostPaidCommonComponents/PostPaidModalTextComponent";
import PostPaidModalBody from "@components/PostPaidCommonComponents/PostPaidModalBody";
const PostPaidJobSeeekerFormModal = ({
  opened,
  activeSubscriptionData,

  setSubscriptionName,
  onClose,
  hForm,
}: {
  opened: boolean;
  onClose: () => void;

  setSubscriptionName: Dispatch<SetStateAction<string | null | undefined>>;
  activeSubscriptionData: PostPaidActiveSubscriptionResponseType[];
  hForm: UseFormReturn<ModifiedCreateJobType>;
}) => {
  const isMobile = useMediaQuery("(max-width: 525px)");

  const { setValue } = hForm;
  const [selectedPlanName, setSelectedPlanName] = useState<string | null>();
  const [selectedPlanId, setSelectedPlanId] = useState<string | null>(
    activeSubscriptionData.length > 0 ? activeSubscriptionData[0].id : null
  );
  const [boostDays, setBoostDays] = useState<number | null>(
    activeSubscriptionData.length > 0
      ? activeSubscriptionData[0].boostDays
      : null
  );

  const handlePromotedJob = () => {
    setValue("subscriptionId", selectedPlanId);
    setSubscriptionName(
      activeSubscriptionData.length > 0
        ? selectedPlanName
          ? selectedPlanName
          : ` ${activeSubscriptionData[0].subscriptionName} - valid for ${activeSubscriptionData[0].validForDays} days`
        : null
    );

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
      <Box w={"100%"} pt={20} pb={30} px={{ base: 10, sm: 40 }}>
        <Stack gap={34}>
          <PostPaidModalHeader />
          <Stack>
            <PostPaidModalTextComponent />
            <Flex
              gap={30}
              maw={520}
              wrap={"wrap"}
              direction={isMobile ? "column" : "row"}
            >
              {activeSubscriptionData.map(
                (
                  data: PostPaidActiveSubscriptionResponseType,
                  index: number
                ) => (
                  <Box key={index}>
                    <CustomPostBoostCard
                      selectedPlanId={selectedPlanId}
                      boostDays={boostDays}
                      setSelectedPlanName={setSelectedPlanName}
                      setBoostDays={setBoostDays}
                      setSelectedPlanId={setSelectedPlanId}
                      data={data}
                    />
                  </Box>
                )
              )}
            </Flex>
          </Stack>
          <PostPaidModalBody
            boostDays={boostDays}
            handlePromotedJob={handlePromotedJob}
          />
        </Stack>
      </Box>
    </CustomModal>
  );
};

export default PostPaidJobSeeekerFormModal;
