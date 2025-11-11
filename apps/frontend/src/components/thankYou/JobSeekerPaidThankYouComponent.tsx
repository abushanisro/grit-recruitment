import { Button, Flex, Grid, Stack } from "@mantine/core";
import React, { useEffect } from "react";
import { useDisclosure, useMediaQuery } from "@mantine/hooks";
import CustomThankYouModal from "@components/CustomThankYouModal";
import { IconDownload } from "@tabler/icons-react";

import { getQueryClient } from "api";
import { contract } from "../../../../contract";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import { isEmpty } from "lodash";
import ErrorMessage from "@components/ErrorMessage";

import classes from "styles/editButton.module.css";
import ThankyouHeaderComponent from "./ThankyouHeaderComponent";
import ThankYouBodyComponent from "./ThankYouBodyComponent";

const ThankYouComponent = () => {
  const [opened, { open, close }] = useDisclosure(false);
  const isMobile = useMediaQuery("(max-width: 426px)");

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
        {/* 1st stack */}

        <ThankYouBodyComponent data={data.body} />
        <Flex
          direction={{ base: "column", sm: "row" }}
          mt={10}
          justify="space-between"
          gap={{ base: 10, sm: 0 }}
        >
          <Button
            component="a"
            href={data.body.invoice_link}
            target="blank"
            className={classes.GoButton}
            bg="primarySkyBlue.6"
            style={{
              border: "1px solid var(--mantine-color-primarySkyBlue.6)",
              borderRadius: "8px",
            }}
            leftSection={<IconDownload size={isMobile ? 14 : 20} />}
            fw={600}
            w={{ base: "100%" }}
            py={10}
            fz={{ base: 14, md: 16 }}
            c="black"
          >
            Download Invoice
          </Button>
        </Flex>
      </Stack>
    </CustomThankYouModal>
  );
};

export default ThankYouComponent;
