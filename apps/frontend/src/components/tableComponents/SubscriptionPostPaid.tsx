import DetailedPaginatedTable from "@components/DetailedPaginatedTable";
import React, { useState } from "react";
import { MySubscriptionPostPaidList } from "@/types/employer";
import { Badge, Center, Grid, Stack, Table, Text } from "@mantine/core";
import { getQueryClient } from "api";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import { contract } from "../../../../contract";
import classes from "styles/actionIcon.module.css";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import ErrorMessage from "@components/ErrorMessage";
import { isEmpty } from "lodash";
import { useQueryState } from "@/hooks/queryState";
import { getIndianDate } from "@/utils/common";
import { PostPaidActiveSubscriptionResponseType } from "@/types/postPaid";

import { useDisclosure } from "@mantine/hooks";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import TablePostPaidJobModal from "@components/tableModals/TablePostPaidJobModal";
import { getColorForPaymentStatus } from "@/utils/colors";

const tableHeaders = [
  {
    name: "Subscription",
    valueFunc: (v: MySubscriptionPostPaidList) => v.subscriptionName,
  },

  {
    name: "Validity",
    valueFunc: (v: MySubscriptionPostPaidList) => v.validForDays,
  },
  {
    name: "Start Date",
    valueFunc: (v: MySubscriptionPostPaidList) => v.createdAt,
  },
  {
    name: "Expiry Date",
    valueFunc: (v: MySubscriptionPostPaidList) => v.expiryDate,
  },

  {
    name: "Pending Job Post Count",
    valueFunc: (v: MySubscriptionPostPaidList) => v.boostLimit,
  },
  {
    name: "Status",
    valueFunc: (v: MySubscriptionPostPaidList) => v.paymentStatus,
  },
];
const MySubscriptionList = () => {
  const { showToast } = useCustomToast();
  const [subscriptionId, setSubscriptionId] = useState<string | null>(null);
  const CustomText = ({ label }: { label: string | number }) => {
    return <Text fz={{ base: 12, sm: 16 }}>{label}</Text>;
  };
  const [
    usedProfileOpened,
    { open: usedProfileOpen, close: usedProfileClose },
  ] = useDisclosure(false);
  const handleBoost = (dataa: PostPaidActiveSubscriptionResponseType) => {
    if (dataa.boostUsed === 0) {
      showToast({
        status: ToastStatus.error,
        message: "You did not boost any job.",
      });
    } else {
      setSubscriptionId(dataa.id);
      usedProfileOpen();
    }
  };

  const [pageNumber, setPageNumber] = useQueryState<number>("pageNumber", 1);
  const queryObj = {
    pageSize: String(5),
    pageNumber: String(pageNumber),
  };
  const { data, isLoading, error } =
    getQueryClient().paidJobs.getActiveSubscriptions.useQuery(
      [contract.paidJobs.getActiveSubscriptions.path, queryObj],
      {
        query: queryObj,
      },
      {
        refetchInterval: 5000,
      }
    );

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
    <>
      {subscriptionId && (
        <TablePostPaidJobModal
          opened={usedProfileOpened}
          onClose={usedProfileClose}
          subscriptionId={subscriptionId}
        />
      )}
      <DetailedPaginatedTable
        tableHeaderSection={tableHeaders}
        tableData={{
          results: data.body.results,
        }}
        tableBody={(data: PostPaidActiveSubscriptionResponseType[]) => {
          if (data.length === 0) {
            return (
              <Table.Tr key="EmptyData">
                <Table.Td>
                  <Center>No Subscription Found</Center>
                </Table.Td>
              </Table.Tr>
            );
          }
          return data.map((payment: PostPaidActiveSubscriptionResponseType) => {
            return (
              <>
                <Table.Tr key={payment.id}>
                  <Table.Td>
                    {payment.subscriptionName ? (
                      <CustomText label={payment.subscriptionName} />
                    ) : (
                      <>-</>
                    )}
                  </Table.Td>
                  <Table.Td>
                    {payment.validForDays ? (
                      <CustomText label={`${payment.validForDays} days`} />
                    ) : (
                      <>-</>
                    )}
                  </Table.Td>
                  <Table.Td>
                    <Stack gap={0}>
                      {payment.createdAt ? (
                        <CustomText label={getIndianDate(payment.createdAt)} />
                      ) : (
                        <>-</>
                      )}
                    </Stack>
                  </Table.Td>
                  <Table.Td>
                    <Stack gap={0}>
                      {payment.expiryDate ? (
                        <CustomText label={getIndianDate(payment.expiryDate)} />
                      ) : (
                        <>-</>
                      )}
                    </Stack>
                  </Table.Td>
                  <Table.Td>
                    <Text fz={{ base: 12, sm: 14 }} ta="center">
                      {payment.boostLimit} available
                    </Text>
                    <Text
                      onClick={() => handleBoost(payment)}
                      fz={{ base: 12, sm: 14 }}
                      ta="center"
                      style={{ cursor: "pointer" }}
                      className={classes.used}
                      c="primaryGreen.4"
                    >
                      {payment.boostUsed} used
                    </Text>
                  </Table.Td>
                  <Table.Td>
                    {payment.paymentStatus ? (
                      <Badge
                        fz={{ base: 8, sm: 10 }}
                        w={80}
                        bg="#FFFFFF26"
                        c={getColorForPaymentStatus(payment.paymentStatus)}
                      >
                        {payment.paymentStatus}
                      </Badge>
                    ) : (
                      <>-</>
                    )}
                  </Table.Td>
                </Table.Tr>
              </>
            );
          });
        }}
        pageNumber={pageNumber}
        setPageNumber={setPageNumber}
        totalPages={data.body.totalPages}
      />
    </>
  );
};

export default MySubscriptionList;
