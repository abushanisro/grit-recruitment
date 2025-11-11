import CustomModal from "@/components/CustomModal";

import { Box, Button, Center, Flex, Grid, Table, Text } from "@mantine/core";

import { useMediaQuery } from "@mantine/hooks";

import classes from "/styles/editButton.module.css";
import { isEmpty } from "lodash";
import { getQueryClient } from "api";

import { getIndianDate } from "@/utils/common";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import { contract } from "../../../../contract";
import { useQueryState } from "@/hooks/queryState";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import ErrorMessage from "@components/ErrorMessage";
import CustomPagination from "@components/CustomPagination";

const TablePostPaidJobModal = ({
  opened,
  onClose,
  subscriptionId,
}: {
  opened: boolean;
  onClose: () => void;

  subscriptionId: string;
}) => {
  const isMobile = useMediaQuery("(max-width: 525px)");
  const [postPaidPageNumber, setPostPaidPageNumber] = useQueryState<number>(
    "postPaidPageNumber",
    1
  );
  const CustomText = ({ label }: { label: string | number }) => {
    return <Text fz={{ base: 12, sm: 16 }}>{label}</Text>;
  };
  const queryObj = {
    pageSize: String(5),
    pageNumber: String(postPaidPageNumber),
    subscriptionId: subscriptionId,
  };

  const { data, isLoading, error } =
    getQueryClient().paidJobs.getBoostedJobsForSubscription.useQuery(
      [contract.paidJobs.getBoostedJobsForSubscription.path, queryObj],
      {
        query: queryObj,
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
    return <CustomErrorMessage errorMessage="You still not boosted any job" />;
  }

  return (
    <CustomModal
      styles={{
        content: {
          background: "var(--mantine-color-primaryDarkBlue-9",
          paddingInline: "20px",
          paddingBlock: "20px",
          border: "1px solid var(--mantine-color-secondarySkyBlue-4)",
          borderRadius: isMobile ? 10 : 30,
        },
      }}
      opened={opened}
      closeOnClickOutside={true}
      onClose={onClose}
    >
      <Flex gap={30} justify="flex-end">
        <Button
          className={classes.GoButton}
          onClick={() => onClose()}
          style={{
            border: "1px solid var(--mantine-color-primarySkyBlue-6)",
            borderRadius: "8px",
            background: "var(--mantine-color-primarySkyBlue-6)",
          }}
          fw={600}
          fz={{ base: 14, md: 16 }}
          c="black"
        >
          Close
        </Button>
      </Flex>
      <Text
        py="10"
        fz={{ md: 18 }}
        fw={700}
        c="var(--mantine-color-primarySkyBlue-6)"
      >
        Jobs boosted by the user :
      </Text>

      <Box w="100%" style={{ overFlowX: "auto" }}>
        <Table.ScrollContainer minWidth={500}>
          <Table
            my={20}
            borderColor="secondaryGreen.1"
            verticalSpacing={"md"}
            horizontalSpacing="lg"
            style={{
              border: "1px solid",
              size: "md",
              borderColor: "var(--mantine-color-secondaryGreen-1)",
              boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-4)",
            }}
          >
            <Table.Thead
              pos="sticky"
              top="-1px"
              style={{
                zIndex: 1,
                borderRadius: "16px",
              }}
            >
              <Table.Tr bg={"var(--mantine-color-primaryPaleBlue-9)"}>
                <Table.Th
                  lh="1.75"
                  fz={"18px"}
                  className={classes.tableHeader}
                  fw={600}
                  c="secondaryGreen.1"
                  style={{
                    whiteSpace: "nowrap",
                  }}
                >
                  Job Name
                </Table.Th>
                <Table.Th
                  lh="1.75"
                  fz={"18px"}
                  className={classes.tableHeader}
                  fw={600}
                  c="secondaryGreen.1"
                  style={{
                    whiteSpace: "nowrap",
                  }}
                >
                  Start Date
                </Table.Th>
                <Table.Th
                  lh="1.75"
                  fz={"18px"}
                  className={classes.tableHeader}
                  fw={600}
                  c="secondaryGreen.1"
                  style={{
                    whiteSpace: "nowrap",
                  }}
                >
                  Expiry Date
                </Table.Th>
              </Table.Tr>
            </Table.Thead>
            {data.body.results.map((jobs, index: number) => (
              <Table.Tbody key={index} style={{ borderRadius: "16px" }}>
                <Table.Tr>
                  <Box key={index}>
                    <Table.Td c="white">
                      {" "}
                      <CustomText label={jobs.title} />
                    </Table.Td>
                  </Box>
                  <Table.Td c="white">
                    {!jobs.boostStartDate ? (
                      <CustomText label="job not approved" />
                    ) : (
                      <CustomText label={getIndianDate(jobs.boostStartDate)} />
                    )}
                  </Table.Td>
                  <Table.Td c="white">
                    {!jobs.boostEndDate ? (
                      <CustomText label="job not approved" />
                    ) : (
                      <CustomText label={getIndianDate(jobs.boostEndDate)} />
                    )}
                  </Table.Td>
                </Table.Tr>
              </Table.Tbody>
            ))}
          </Table>
        </Table.ScrollContainer>
      </Box>

      <Center mt={20}>
        <CustomPagination
          size={isMobile ? "sm" : "md"}
          value={postPaidPageNumber}
          onChange={(e: number) => {
            setPostPaidPageNumber(e);
          }}
          total={data.body.totalPages}
        />
      </Center>
    </CustomModal>
  );
};

export default TablePostPaidJobModal;
