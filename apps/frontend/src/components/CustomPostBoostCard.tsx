import { Badge, Stack, Text, Flex, Center } from "@mantine/core";
import { IconCircleCheckFilled } from "@tabler/icons-react";
import React from "react";
import { IconArrowBigUpLineFilled } from "@tabler/icons-react";
import { Dispatch, SetStateAction } from "react";
import classes from "styles/editButton.module.css";
import { PostPaidActiveSubscriptionResponseType } from "@/types/postPaid";
const CustomPostBoostCard = ({
  boostDays,
  setBoostDays,

  setSelectedPlanName,
  data,
  setSelectedPlanId,
  selectedPlanId,
}: {
  data: PostPaidActiveSubscriptionResponseType;
  boostDays: number | null;

  setSelectedPlanName?: Dispatch<SetStateAction<string | null | undefined>>;
  setBoostDays: Dispatch<SetStateAction<number | null>>;
  setSelectedPlanId: Dispatch<SetStateAction<string | null>>;
  selectedPlanId: string | null;
}) => {
  const handleSelectSubscriptionId = () => {
    setSelectedPlanId(data.id);
    setBoostDays(data.boostDays);
    if (setSelectedPlanName) {
      setSelectedPlanName(
        `${data.subscriptionName} - valid for ${data.boostDays} days`
      );
    }
  };
  const planId = selectedPlanId === data.id;
  return (
    <Stack
      onClick={handleSelectSubscriptionId}
      className={classes.CustomBoostCards}
      style={{
        border: planId
          ? "2px solid var(--mantine-color-primaryGreen-1)"
          : "1px solid var(--mantine-color-primaryGreen-1)",
        borderRadius: "10px",
        cursor: "pointer",
        boxShadow: planId
          ? "0px 2px 2px 0px var(--mantine-color-primaryGreen-3)"
          : "0px 0px 0px 0px var(--mantine-color-primaryGreen-1)",
      }}
      bg={
        planId
          ? "linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
          : "transparent"
      }
      pos="relative"
      pb={20}
      pt={planId ? 4 : 20}
      px={20}
    >
      {planId && (
        <Flex justify="center">
          <Flex
            justify="center"
            align="center"
            pos="absolute"
            top="-18px"
            style={{ borderRadius: "50%" }}
            bg={"#25282E"}
            w={30}
            h={30}
          >
            <IconCircleCheckFilled size={15} style={{ color: " #2AAF57" }} />
          </Flex>
        </Flex>
      )}
      <Text
        c="var(--mantine-color-primarySkyBlue-6)"
        fz={{ base: 14, sm: 16 }}
        fw={700}
      >
        {data.subscriptionName}
      </Text>
      <Center>
        <Stack>
          <Badge
            fz={{ base: 12, sm: 12 }}
            w={160}
            fw={400}
            tt="none"
            bg="#FFFFFF26"
            leftSection={<IconArrowBigUpLineFilled color="white" size={12} />}
          >
            {data.boostLimit} job posts left
          </Badge>
          <Badge
            fz={{ base: 12, sm: 12 }}
            w={160}
            fw={400}
            tt="none"
            bg="#FFFFFF26"
            leftSection={<IconArrowBigUpLineFilled color="white" size={12} />}
          >
            {data.boostDays} days boost
          </Badge>
        </Stack>
      </Center>
    </Stack>
  );
};

export default CustomPostBoostCard;
