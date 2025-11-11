import React from "react";
import { Divider, SimpleGrid, Stack, Text } from "@mantine/core";
import { CustomStack, CustomText } from "./PostPaidThankYouComponent";
import { getLastPaymentDetailsResponseType } from "@/types/employer";

const ThankYouBodyComponent = ({
  data,
}: {
  data: getLastPaymentDetailsResponseType;
}) => {
  return (
    <>
      <Stack gap={0} align="center">
        <Text fz={{ base: 16, sm: 24 }} c="#48ff68">
          Payment Success!
        </Text>
        <Text
          style={{ wordBreak: "break-all" }}
          fz={{ base: 12, sm: 16 }}
          c="#fff"
        >
          Your payment has been successfully done.
        </Text>
      </Stack>
      <Divider color="#FFFFFF29" w="100%" />
      <Stack gap={0} align="center">
        <Text fz={{ base: 12, sm: 16 }} c="#FFFFFFB8">
          Total Payment
        </Text>
        <Text fz={{ base: 16, sm: 24 }} fw={600} c="secondaryGreen.1">
          INR {data.amount}
        </Text>
      </Stack>

      <SimpleGrid cols={2} spacing={20} verticalSpacing={20}>
        <CustomStack>
          <CustomText label={"Ref Number"} />
          <Text
            style={{ wordBreak: "break-all" }}
            fz={{ base: 12, sm: 16 }}
            c="#FFFFFF"
          >
            {data.reference_no}
          </Text>
        </CustomStack>

        <CustomStack>
          <CustomText label={"Payment Time"} />
          <Text fz={{ base: 12, md: 16 }} c="#FFFFFF">
            {new Date(data.payment_date).toLocaleString("en-IN", {
              timeZone: "Asia/Kolkata", // Indian Standard Time (IST)
              year: "numeric",
              month: "2-digit",
              day: "2-digit",
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            })}
          </Text>
        </CustomStack>

        <Stack
          gap={0}
          p={10}
          style={{ border: "1px solid #FFFFFF29", borderRadius: "8px" }}
        >
          <CustomText label={"Payment Method"} />
          <Text fz={{ base: 12, md: 16 }} c="#FFFFFF">
            {data.payment_method}
          </Text>
        </Stack>

        <CustomStack>
          <CustomText label={"Product"} />
          <Text fz={{ base: 12, md: 16 }} c="#FFFFFF">
            Job seeker directory {data.subscription_validity} days access
          </Text>
        </CustomStack>
      </SimpleGrid>
    </>
  );
};

export default ThankYouBodyComponent;
