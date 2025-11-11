import { Grid, Stack, Text, Button, TextProps } from "@mantine/core";
import React, { useEffect } from "react";
import { useDisclosure } from "@mantine/hooks";
import CustomThankYouModal from "@components/CustomThankYouModal";
import { getQueryClient } from "api";
import { contract } from "../../../../contract";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import { isEmpty } from "lodash";
import ErrorMessage from "@components/ErrorMessage";
import ThankyouHeaderComponent from "./ThankyouHeaderComponent";
import ThankYouBodyComponent from "./ThankYouBodyComponent";
import ThankYouButtonComponent from "./ThankYouButtonComponent";
import classes from "styles/editButton.module.css";
import Link from "next/link";

export const CustomText = ({
  label,
  ...props
}: { label: string } & TextProps) => {
  return (
    <Text fz={{ base: "12", sm: "16" }} c="#FFFFFFB8" {...props}>
      {label}
    </Text>
  );
};

export const CustomStack = ({ children }: { children: React.ReactNode }) => {
  return (
    <Stack
      gap={4}
      p={10}
      style={{ border: "1px solid #FFFFFF29", borderRadius: "8px" }}
    >
      {children}
    </Stack>
  );
};

const PostPaidThankYouComponent = () => {
  const [opened, { open, close }] = useDisclosure(false);
  useEffect(() => {
    open();
  }, []);

  const { data, isLoading, error } =
    getQueryClient().paidJobs.getLastPaymentDetails.useQuery([
      contract.paidJobs.getLastPaymentDetails.path,
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
            <Link href='subscriptionDetails/?activeTab="Paid+Job+Post"'>
              {" "}
              <Button
                bg="primarySkyBlue.6"
                style={{
                  border: "1px solid var(--mantine-color-primarySkyBlue-6)",
                  borderRadius: "8px",
                }}
                fw={600}
                className={classes.GoButton}
                w={{ base: "100%", sm: "100%" }}
                fz={{ base: 14, md: 16 }}
                py={10}
                c="black"
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

export default PostPaidThankYouComponent;
