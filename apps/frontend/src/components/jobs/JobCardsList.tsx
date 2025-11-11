import { Center, Grid } from "@mantine/core";
import { Fragment, useEffect } from "react";
import JobCard from "@/components/jobs/jobcard";
import { getQueryClient } from "api";
import { UseFormReturn } from "react-hook-form";
import { FilterInterface } from "@/types/jobs";
import CreateProfile from "@/components/jobs/CreateProfile";
import { EmploymentMode, EmploymentType, UserRole } from "contract/enum";
import isEmpty from "lodash/isEmpty";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import { useRouter } from "next/router";
import { useLogin } from "@/contexts/LoginProvider";
import { contract } from "contract";
import ErrorMessage from "@components/ErrorMessage";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import { useQueryClient } from "@tanstack/react-query";
import { useApi } from "@/hooks/useApi";
import { useUserData } from "@/contexts/UserProvider";
import CustomPagination from "@/components/CustomPagination";

const getEmploymentType = (value: string) => {
  switch (value) {
    case EmploymentType.Internship:
      return EmploymentType.Internship;
    case EmploymentType.PartTime:
      return EmploymentType.PartTime;
    case EmploymentType.FullTime:
    default:
      return EmploymentType.FullTime;
  }
};
export const getEmploymentMode = (value: string) => {
  switch (value) {
    case EmploymentMode.Hybrid:
      return EmploymentMode.Hybrid;
    case EmploymentMode.Onsite:
      return EmploymentMode.Onsite;
    case EmploymentMode.Remote:
    default:
      return EmploymentMode.Remote;
  }
};
const JobCardsList = ({
  hForm,
}: {
  hForm: UseFormReturn<FilterInterface, any, undefined>;
}) => {
  const router = useRouter();
  const { pathname, query } = router;
  const { isLoggedIn } = useLogin();
  const scrollToJobId = String(query.jobId ?? "");
  const { watch, setValue } = hForm;
  const mutateQueryClient = useQueryClient();
  const { makeApiCall } = useApi();
  const { userDetails } = useUserData();
  const pageNumber = watch("pageNumber");
  const pageSize = watch("pageSize");
  const searchText = watch("searchText");
  const salary = watch("salary");
  const employementType = watch("employmentType");
  const employementMode = watch("workSchedule");
  const experience = watch("experience");
  const location = watch("location");
  const industry = watch("industry");
  const subFunctions = watch("subFunctions");
  const queryObj = {
    pageSize: String(pageSize),
    pageNumber: String(pageNumber),
    searchText: searchText,
    minSalaryInLpa:
      salary && salary.value.split("-")[0]
        ? salary.value.split("-")[0]
        : undefined,
    maxSalaryInLpa:
      salary && salary.value.split("-")[1]
        ? salary.value.split("-")[1]
        : undefined,
    employmentType: employementType
      ? employementType?.map((eachValue) => {
          return getEmploymentType(eachValue.value);
        })
      : undefined,
    employmentMode: employementMode
      ? employementMode?.map((eachValue) => {
          return getEmploymentMode(eachValue.value);
        })
      : undefined,
    cityIds: location
      ? location.map((eachValue) => {
          return eachValue.value;
        })
      : undefined,
    industry: industry
      ? industry.map((eachValue) => {
          return eachValue.value;
        })
      : undefined,
    subfunctionIds: subFunctions
      ? subFunctions.map((eachValue) => {
          return eachValue.value;
        })
      : undefined,
    minExperienceInYear:
      experience && experience.value.split("-")[0]
        ? experience.value.split("-")[0]
        : undefined,
    maxExperienceInYear:
      experience && experience.value.split("-")[1]
        ? experience.value.split("-")[1]
        : undefined,
  };
  const { data, isLoading, error } = getQueryClient().job.filter.useQuery(
    [contract.job.filter.path, queryObj],
    {
      query: queryObj,
    }
  );
  const saveJob = ({ jobId }: { jobId: number }) => {
    makeApiCall({
      fetcherFn: async () => {
        const response = await getQueryClient().job.saveJob.mutation({
          body: {
            jobId: Number(jobId),
          },
        });
        return response;
      },
      successMsgProps: { message: "Job saved successfully" },
      onSuccessFn: () => {
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.job.filter.path],
        });
      },
      showFailureMsg: true,
    });
  };
  useEffect(() => {
    if (!router.isReady || !data || !scrollToJobId) {
      return;
    }
    if (isLoggedIn && userDetails && userDetails.role === UserRole.jobSeeker) {
      saveJob({ jobId: Number(scrollToJobId) });
    }

    const elementToScroll = document.getElementById(`job-id-${scrollToJobId}`);
    if (elementToScroll) {
      elementToScroll.scrollIntoView({ behavior: "smooth", block: "center" });

      delete query.jobId;
      router.replace({ pathname, query }, undefined, { shallow: true });
    }
  }, [router.isReady, scrollToJobId, data]);

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
  if (isEmpty(data.body.results)) {
    return <CustomErrorMessage errorMessage="No jobs found" />;
  }
  return (
    <>
      <Grid
        maw={{ base: 400, sm: 500, md: 700, lg: 1100 }}
        style={{ overflow: "visible" }}
        gutter={{ base: 8, md: 58 }}
        pl={{ base: 20, xl: 30 }}
      >
        <Grid.Col span={{ base: 12, lg: 7.5 }}>
          {data.body.results.map((eachJob, index: number) => {
            return (
              <Fragment key={index}>
                <JobCard
                  data={eachJob}
                  showSaved={true}
                  isSaved={eachJob.isSaved}
                />
              </Fragment>
            );
          })}
        </Grid.Col>
        <Grid.Col
          span={{ base: 0, lg: 4.5 }}
          pos="sticky"
          top={400}
          display={isLoggedIn ? "none" : { base: "none", lg: "block" }}
        >
          <CreateProfile />
        </Grid.Col>
      </Grid>

      <Center>
        <CustomPagination
          value={pageNumber}
          onChange={(e) => {
            setValue("pageNumber", e);
          }}
          total={data.body.totalPages}
        />
      </Center>
    </>
  );
};
export default JobCardsList;
