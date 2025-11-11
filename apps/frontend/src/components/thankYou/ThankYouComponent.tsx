import { Grid, Stack, Button } from "@mantine/core";
import React, { useEffect } from "react";
import { useDisclosure } from "@mantine/hooks";
import { getQueryClient } from "api";
import { contract } from "contract";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import CustomThankYouModal from "@components/CustomThankYouModal";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import isEmpty from "lodash/isEmpty";
import ErrorMessage from "@components/ErrorMessage";
import ThankyouHeaderComponent from "@components/thankYou/ThankyouHeaderComponent";
import ThankYouBodyComponent from "@components/thankYou/ThankYouBodyComponent";
import ThankYouButtonComponent from "@components/thankYou/ThankYouButtonComponent";
import classes from "styles/editButton.module.css";
import Link from "next/link";

const ThankYouComponent = () => {
  const [opened, { open, close }] = useDisclosure(false);

  useEffect(() => {
    open();
  }, []);

  const { data, isLoading, error } =
    getQueryClient().subscription.getLastPaymentDetails.useQuery([
      contract.subscription.getLastPaymentDetails.path,
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
    return <CustomErrorMessage errorMessage="No Subscription Plans found" />;
  }

  return (
    <CustomThankYouModal pos="relative" opened={opened} onClose={close}>
      <ThankyouHeaderComponent />

      <Stack mt={10} gap={10}>
        <ThankYouBodyComponent data={data.body} />

        <ThankYouButtonComponent
          goToButton={
            <Link href="/subscriptionDetails">
              <Button
                bg="primarySkyBlue.6"
                style={{
                  border: "1px solid var(--mantine-color-primarySkyBlue-6)",
                  borderRadius: "8px",
                }}
                fw={600}
                className={classes.GoButton}
                fz={{ base: 16, md: 18 }}
                h={44}
                py={10}
                c="black"
                w={"100%"}
              >
                Go To Subscription
              </Button>
            </Link>
          }
          invoiceLink={data.body.invoice_link}
        />
      </Stack>
    </CustomThankYouModal>
  );
};

export default ThankYouComponent;
