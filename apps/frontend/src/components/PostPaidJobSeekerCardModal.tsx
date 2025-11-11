import CustomModal from "@/components/CustomModal";

import { Box, Stack, Flex } from "@mantine/core";

import { useMediaQuery } from "@mantine/hooks";

import CustomPostBoostCard from "@components/CustomPostBoostCard";
import { useState } from "react";
import { useApi } from "@/hooks/useApi";
import { getQueryClient } from "api";

import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { PostPaidActiveSubscriptionResponseType } from "@/types/postPaid";
import { useQueryClient } from "@tanstack/react-query";
import { contract } from "../../../contract";
import PostPaidModalHeader from "@components/PostPaidCommonComponents/postPaidModalHeader";
import PostPaidModalTextComponent from "@components/PostPaidCommonComponents/PostPaidModalTextComponent";
import PostPaidModalButtonLoaderComponent from "@components/PostPaidCommonComponents/PostPaidModalButtonLoaderComponent";

const PostPaidJobSeekerCardModal = ({
  opened,
  onClose,
  activeSubscriptionData,
  jobId,
}: {
  opened: boolean;
  onClose: () => void;
  activeSubscriptionData: PostPaidActiveSubscriptionResponseType[];
  jobId: number;
}) => {
  const isMobile = useMediaQuery("(max-width: 525px)");

  const [selectedPlanId, setSelectedPlanId] = useState<string | null>(
    activeSubscriptionData.length > 0 ? activeSubscriptionData[0].id : null
  );
  const [boostDays, setBoostDays] = useState<number | null>(
    activeSubscriptionData.length > 0
      ? activeSubscriptionData[0].boostDays
      : null
  );

  const mutateQueryClient = useQueryClient();
  const [loader, setLoader] = useState<boolean>(false);
  const { showToast } = useCustomToast();
  const { makeApiCall } = useApi();
  const handlePromotedJob = () => {
    setLoader(true);
    const queryObj = {
      jobId: String(jobId),
      subscriptionId: String(selectedPlanId),
    };
    makeApiCall({
      fetcherFn: async () => {
        const response = await getQueryClient().paidJobs.boostJob.mutation({
          body: queryObj,
        });
        return response;
      },
      onSuccessFn: (response) => {
        if (response) {
          showToast({
            status: ToastStatus.success,
            message: "Job is Successfully Boosted",
          });
        }
        onClose();
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.job.fetchJobsByEmployer.path],
        });
      },
      finallyFn: () => {
        setLoader(false);
      },
    });
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
                      setBoostDays={setBoostDays}
                      setSelectedPlanId={setSelectedPlanId}
                      data={data}
                    />
                  </Box>
                )
              )}
            </Flex>
          </Stack>
          <PostPaidModalButtonLoaderComponent
            boostDays={boostDays}
            handlePromotedJob={handlePromotedJob}
            loader={loader}
          />
        </Stack>
      </Box>
    </CustomModal>
  );
};

export default PostPaidJobSeekerCardModal;
