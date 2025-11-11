import { Center } from "@mantine/core";
import { getQueryClient } from "api";
import isEmpty from "lodash/isEmpty";
import { Fragment } from "react";
import { useQueryState } from "@/hooks/queryState";
import CustomErrorMessage from "@/components/jobs/CustomErrorMessage";
import SavedJobsCard from "@/components/jobs/jobcard/SavedJobsCard";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import { contract } from "contract";
import ErrorMessage from "@components/ErrorMessage";
import { getTimeFromNow } from "@/utils/common";
import CustomPagination from "@/components/CustomPagination";

const SavedJobsList = () => {
  const pageSize = 20;
  const [pageNumber, setPageNumber] = useQueryState<number>(
    "savedJobsPageNumber",
    1
  );

  const { data, isLoading, error } =
    getQueryClient().job.fetchSavedJobs.useQuery(
      [contract.job.fetchSavedJobs.path, pageSize, pageNumber],
      {
        query: { pageSize: String(pageSize), pageNumber: String(pageNumber) },
      }
    );
  if (isLoading) {
    return <JobListSkeletonCard />;
  }
  if (error) {
    return <ErrorMessage error={error} />;
  }
  if (isEmpty(data.body.results)) {
    return <CustomErrorMessage errorMessage=" No jobs found" />;
  }
  return (
    <>
      {data.body.results.map((eachJob, index: number) => {
        return (
          <Fragment key={index}>
            <SavedJobsCard
              data={eachJob}
              badgeTimeAgoLabel={`Saved ${getTimeFromNow(
                eachJob.jobSavedTime
              )}`}
            />
          </Fragment>
        );
      })}

      <Center>
        <CustomPagination
          value={pageNumber}
          onChange={(e) => {
            setPageNumber(e);
          }}
          total={data.body.totalPages}
        />
      </Center>
    </>
  );
};
export default SavedJobsList;
