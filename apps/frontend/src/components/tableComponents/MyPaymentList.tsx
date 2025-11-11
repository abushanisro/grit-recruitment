import DetailedPaginatedTable from "@components/DetailedPaginatedTable";
import React from "react";
import { MyPaymentHeading, getSubscriptionHistoryType } from "@/types/employer";
import { Badge, Center, Grid, Group, Stack, Table, Text } from "@mantine/core";
import { IconDownload } from "@tabler/icons-react";
import classes from "styles/actionIcon.module.css";
import { getQueryClient } from "api";
import { contract } from "../../../../contract";
import ErrorMessage from "@components/ErrorMessage";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import { isEmpty } from "lodash";
import { useQueryState } from "@/hooks/queryState";
import { getIndianDate } from "@/utils/common";
import { getColorForPaymentStatus } from "@/utils/colors";

const tableHeaders = [
  {
    name: "Subscription",
    valueFunc: (v: MyPaymentHeading) => v.subscriptionName,
  },

  {
    name: "Date",
    valueFunc: (v: MyPaymentHeading) => v.createdAt,
  },
  {
    name: "Amount",
    valueFunc: (v: MyPaymentHeading) => v.paidAmount,
  },
  {
    name: "Payment Method",
    valueFunc: (v: MyPaymentHeading) => v.paymentMethod,
  },
  {
    name: "Status",
    valueFunc: (v: MyPaymentHeading) => v.paymentStatus,
  },
  {
    name: "Action",
    valueFunc: (v: MyPaymentHeading) => v.invoiceLink,
  },
];

const MyPaymentList = () => {
  const [pageNumber, setPageNumber] = useQueryState<number>("pageNumber", 1);

  const CustomText = ({ label }: { label: string | number }) => {
    return <Text fz={{ base: 12, sm: 16 }}>{label}</Text>;
  };

  const queryObj = {
    pageSize: String(5),
    pageNumber: String(pageNumber),
  };

  const { data, isLoading, error } =
    getQueryClient().subscription.getSubscriptionHistory.useQuery(
      [contract.subscription.getSubscriptionHistory.path, queryObj],
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
    <DetailedPaginatedTable
      tableHeaderSection={tableHeaders}
      totalPages={data.body.totalPages}
      tableData={{
        results: data.body.results,
      }}
      tableBody={(data: getSubscriptionHistoryType[]) => {
        if (data.length === 0) {
          return (
            <Table.Tr key="EmptyData">
              <Table.Td>
                <Center>No Payment Found</Center>
              </Table.Td>
            </Table.Tr>
          );
        }
        return data.map((payment: getSubscriptionHistoryType) => {
          return (
            <Table.Tr key={payment.id}>
              <Table.Td>
                {payment.subscriptionName ? (
                  <Text fz={{ base: 14, sm: 16 }}>
                    {payment.subscriptionName}
                  </Text>
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
                {payment.paidAmount ? (
                  <CustomText label={`₹ ${payment.paidAmount}`} />
                ) : (
                  <>-</>
                )}
              </Table.Td>
              <Table.Td>
                {payment.paymentMethod ? (
                  <CustomText label={payment.paymentMethod} />
                ) : (
                  <>-</>
                )}
              </Table.Td>
              <Table.Td>
                {payment.paymentStatus ? (
                  <Badge
                    fz={12}
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
              <Table.Td>
                {payment.invoiceLink ? (
                  <a
                    style={{ textDecoration: "none", color: "white" }}
                    href={payment.invoiceLink}
                    target="blank"
                  >
                    {payment.paymentStatus == "pending" ? (
                      <Group className={classes.download}>
                        <CustomText label={" ₹ Pay Now "} />
                      </Group>
                    ) : (
                      <Group className={classes.download} gap={4}>
                        <IconDownload size={18} />
                        <CustomText label={"Download"} />
                      </Group>
                    )}
                  </a>
                ) : (
                  <>-</>
                )}
              </Table.Td>
            </Table.Tr>
          );
        });
      }}
      pageNumber={pageNumber}
      setPageNumber={setPageNumber}
    />
  );
};

export default MyPaymentList;
