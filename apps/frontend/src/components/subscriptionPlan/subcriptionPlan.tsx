import React from "react";
import {
  Box,
  Divider,
  Grid,
  Group,
  Image,
  Paper,
  Stack,
  StackProps,
  Text,
} from "@mantine/core";
import CustomCards from "@components/CustomCards";

import PrimaryButton from "@components/buttons/PrimaryButton";
import { useMediaQuery } from "@mantine/hooks";

import { subscriptionPlansType } from "@/types/employer";
import ErrorMessage from "@components/ErrorMessage";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import { getQueryClient } from "api";
import { contract } from "contract";

import { isEmpty } from "lodash";
import { useApi } from "@/hooks/useApi";

import { useUserData } from "@/contexts/UserProvider";
import { razorPayKey } from "@/utils/common";
import { getApiUrl } from "@/env";

export const CustomGroupComponent = ({
  label,
  image,
}: {
  label: string;
  image: string;
}) => {
  return (
    <Group align="center" justify="flex-start">
      <Image src={image} alt={`icon`} w={32} />
      <Text c="white" maw={200} fz={{ base: 14, md: 16 }} fw={400}>
        {label}
      </Text>
    </Group>
  );
};

export const PlanTitleComponent = () => {
  return (
    <Text
      c="white"
      fz={{ base: 24, sm: 28, lg: 40 }}
      ta={{ base: "left", sm: "center" }}
      fw="600"
    >
      Select Subscription Plans
    </Text>
  );
};
export const PlanHeaderComponent = ({
  name,
  price,
  ...props
}: { name: string; price: number } & StackProps) => {
  return (
    <>
      <Stack align="center" gap={2} {...props}>
        <Group gap={4}>
          <Image
            src="/images/rupeePurple.svg"
            alt="icon"
            w={{ base: "12", sm: "16" }}
          />
          <Text fw={600} fz={{ base: 26, md: 32 }} c="secondaryGreen.1">
            {price}
          </Text>
        </Group>
        <Text fz={{ base: 20, md: 28 }} c="primarySkyBlue.6" fw="600">
          {name}
        </Text>
      </Stack>
      <Divider
        bg="linear-gradient(90deg, rgba(241, 205, 124, 0.10) 17.76%, #22D1DC 55.6%, rgba(241, 205, 124, 0.00) 87.19%)"
        w="100%"
        h="1px"
        color="none"
      />
    </>
  );
};
const SubcriptionPlan = () => {
  const { userDetails } = useUserData();
  const { makeApiCall } = useApi();

  const isMobile = useMediaQuery("(max-width: 426px)");

  const handleBuy = (plans: subscriptionPlansType) => {
    const queryObj = {
      subscriptionPlanId: String(plans.id),
    };

    makeApiCall({
      fetcherFn: async () => {
        const response = await getQueryClient().subscription.getOrder.mutation({
          body: queryObj,
        });
        return response;
      },
      onSuccessFn: (response) => {
        if (response.status !== 201) {
          return;
        }

        var rzp1 = (window as any).Razorpay({
          key: razorPayKey, // Enter the Key ID generated from the Dashboard
          amount: plans.price, // Amount is in currency subunits. Default currency is INR. Hence, 50000 refers to 50000 paise
          currency: "INR",
          name: `${userDetails ? userDetails.companyName : ""}`, //your business name
          description: "Test Transaction",
          image: "https://example.com/your_logo",
          order_id: response.body.orderId,

          callback_url: getApiUrl() + "/subscriptions/updateSubscription",
          //  getApiUrl()+

          prefill: {
            //We recommend using the prefill parameter to auto-fill customer's contact information especially their phone number
            name: `${userDetails ? userDetails.firstName : ""}+{" "} +${
              userDetails ? userDetails.lastName : ""
            }`, //your customer's name
            email: `${userDetails ? userDetails.email : ""}`,
            contact: `${userDetails ? userDetails.phone : ""}`, //Provide the customer's phone number for better conversion rates
          },
        });

        return rzp1.open();
      },
      showFailureMsg: true,
    });
  };

  const { data, isLoading, error } =
    getQueryClient().subscription.getSubscriptionPlans.useQuery([
      contract.subscription.getSubscriptionPlans.path,
    ]);

  if (isLoading) {
    return (
      <Grid
        maw={{ base: 400, sm: 500, md: 700, lg: 1100 }}
        mx="auto"
        my={{ base: 40, md: 58 }}
      >
        <Grid.Col span={{ base: 12, lg: 8 }}>
          <JobListSkeletonCard />
        </Grid.Col>
      </Grid>
    );
  }
  if (error) {
    return <ErrorMessage error={error} />;
  }

  if (isEmpty(data.body)) {
    return <CustomErrorMessage errorMessage="No Subscription found" />;
  }
  return (
    <Box mt="40">
      <PlanTitleComponent />
      <CustomCards>
        {data.body.map((plans, index: number) => (
          <Paper
            px={{ base: 16, sm: 20 }}
            mt={40}
            py={30}
            bg={"primaryDarkBlue.9"}
            style={{
              borderRadius: isMobile ? 10 : 24,
              border: "1px solid",
              borderColor: "var(--mantine-color-secondaryGreen-1)",
              boxShadow:
                " 0px 4px 4px 0px var(--mantine-color-primaryGreen-3) ",
            }}
            key={index}
          >
            <Stack gap={isMobile ? 16 : 20}>
              <PlanHeaderComponent name={plans.name} price={plans.price} />

              <Stack>
                <CustomGroupComponent
                  label={`${plans.allowedProfileCount} Profile views per requirement`}
                  image={"/images/profileView.svg"}
                />
                <CustomGroupComponent
                  label={`${plans.validForDays} days alloted for subscription`}
                  image={"/images/calender.svg"}
                />
              </Stack>
              <PrimaryButton
                onClick={() => handleBuy(plans)}
                label="Buy Now"
                fw={600}
                fz={16}
                w="100%"
              />
            </Stack>
          </Paper>
        ))}
      </CustomCards>
    </Box>
  );
};

export default SubcriptionPlan;
