import PageLayout from "@components/layouts/PageLayout";
import SectionContainer from "@components/SectionContainer";
import { Box, Center, Text, Stack } from "@mantine/core";
import { useQueryState } from "@/hooks/queryState";
import { JobApplicationStatus, NoticePeriodEnum } from "contract/enum";
import { getQueryClient } from "api";
import { contract } from "contract";
import ErrorMessage from "@components/ErrorMessage";
import {
  JobSeekerApplicantsResponse,
  ManageJobseekerJobsPaginatedListType,
} from "@/types/jobs";
import JobSeekerApplicantCard from "@components/jobs/manageJobs/JobSeekerApplicantCard";
import { Fragment, useEffect, useState } from "react";
import { ErrorHttpStatusCode } from "@ts-rest/core";
import isNil from "lodash/isNil";
import isEmpty from "lodash/isEmpty";
import { useForm, UseFormReturn } from "react-hook-form";
import JobSeekerApplicantFilters from "@components/jobs/manageJobs/JobSeekerApplicantFilters";
import { useDisclosure, useMediaQuery } from "@mantine/hooks";
import { useRouter } from "next/router";
import Navbar from "@components/Navbar";
import dynamic from "next/dynamic";
import JobSeekerApplicantMobileCard from "@components/jobs/manageJobs/JobSeekerApplicantMobileCard";
import { useApi } from "@/hooks/useApi";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { useQueryClient } from "@tanstack/react-query";
import { getFullName } from "@/utils/common";
import ManageJobApplicantTabSection from "@components/jobs/manageJobs/ManageApplicantTabSection";
import CoverLetterModal from "@components/CoverLetterModal";
import JobListSkeletonCard from "@components/JobListSkeletonCard";
import CustomPagination from "@components/CustomPagination";
import { JobSeekerFilterRequest } from "@/types/employer";

const ConfirmationModal = dynamic(
  () => import("@components/ConfirmationModal")
);

const paginationPageSize: number = 20;

export const defaultJobSeekerApplicantFilters: JobSeekerFilterRequest = {
  pageNumber: 1,
  status: JobApplicationStatus.Pending,
  experience: null,
  ctc: null,
  locationIds: [],
  noticePeriod: [],
  languageIds: [],
  skillIds: [],
  jobSeekerName: "",
};

const ManageJobSeekerApplicants = () => {
  const router = useRouter();
  const jobId = String(router.query.jobId ?? "");
  const [selectedFilters, setSelectedFilters] =
    useQueryState<JobSeekerFilterRequest>(
      "filters",
      defaultJobSeekerApplicantFilters
    );

  const hForm = useForm<JobSeekerFilterRequest>({
    mode: "onChange",
    defaultValues: selectedFilters,
  });
  const formData = hForm.watch();

  useEffect(() => {
    hForm.reset(selectedFilters);
  }, [router.isReady]);

  //* NOTE: Need to refine this useEffect
  useEffect(() => {
    if (!router.isReady) {
      return;
    }
    const formData = hForm.watch(); // * NOTE: To maintain pageNumber state when refresh
    const { pageNumber, ...otherFilters } = selectedFilters;

    const otherFiltersChanged = Object.keys(otherFilters).some((key) => {
      const inferedKey = key as keyof Omit<
        JobSeekerFilterRequest,
        "pageNumber"
      >;
      return (
        JSON.stringify(formData[inferedKey]) !==
        JSON.stringify(selectedFilters[inferedKey])
      );
    });

    if (otherFiltersChanged) {
      setSelectedFilters({ ...formData, pageNumber: 1 });
      hForm.setValue("pageNumber", 1);
    } else if (formData.pageNumber !== selectedFilters.pageNumber) {
      setSelectedFilters(formData);
    }
  }, [router.isReady, JSON.stringify(formData)]);

  return (
    <PageLayout
      navbarComponent={
        <Navbar>
          <JobSeekerApplicantFilters hForm={hForm} title="Filters" />
        </Navbar>
      }
      mainComponent={
        <ManageJobSeekerApplicantsPage hForm={hForm} jobId={jobId} />
      }
    />
  );
};

const ManageJobSeekerApplicantsPage = ({
  jobId,
  hForm,
}: {
  jobId: string;
  hForm: UseFormReturn<JobSeekerFilterRequest>;
}) => {
  const { watch, setValue } = hForm;
  const filterData = watch();
  const filterCtc = filterData.ctc;
  const filterExperience = filterData.experience;

  const {
    data: paginatedApplicantsList,
    isLoading,
    error,
  } = getQueryClient().job.manageJobApplicationsOfJobseekers.useQuery(
    [
      contract.job.manageJobApplicationsOfJobseekers.path,
      filterData.pageNumber,
      filterData.status,
      // filterData.jobTitle,
      filterExperience,
      filterCtc,
      filterData.locationIds,
      filterData.noticePeriod,
      filterData.jobSeekerName,
      filterData.skillIds,
      filterData.languageIds,
      jobId,
    ],
    {
      query: {
        ...filterData,
        pageSize: String(paginationPageSize),
        pageNumber: String(filterData.pageNumber),
        status: filterData.status as JobApplicationStatus,
        minSalaryInLpa:
          filterCtc && filterCtc.value && filterCtc.value.split("-")[0]
            ? filterCtc?.value.split("-")[0]
            : undefined,
        maxSalaryInLpa:
          filterCtc && filterCtc.value && filterCtc.value.split("-")[1]
            ? filterCtc.value.split("-")[1]
            : undefined,
        noticePeriod: filterData.noticePeriod.map(
          (period) => period.value as NoticePeriodEnum
        ),
        locationIds: filterData.locationIds.map((loc) => loc.value),
        minExperienceInYears:
          filterExperience && filterExperience.value.split("-")[0]
            ? filterExperience.value.split("-")[0]
            : undefined,
        maxExperienceInYears:
          filterExperience && filterExperience.value.split("-")[1]
            ? filterExperience.value.split("-")[1]
            : undefined,
        jobId: jobId,
        languageIds: filterData.languageIds?.map((lang) => lang.value) ?? [],
        skillIds: filterData.skillIds?.map((skill) => skill.value) ?? [],
      },
    }
  );
  return (
    <Box mt={{ base: 24, md: 32, lg: 40 }}>
      <Text fz={{ base: 28, sm: 32 }} fw={600} style={{ letterSpacing: "2px" }}>
        Jobseeker
      </Text>
      <Box display={{ base: "none", sm: "block" }}>
        <JobSeekerApplicantFilters hForm={hForm} />
      </Box>
      <Box my={40}>
        <ManageJobApplicantTabSection
          selectedStatus={filterData.status}
          countData={
            isNil(paginatedApplicantsList)
              ? null
              : {
                  pending: paginatedApplicantsList.body.pendingCount,
                  shortlist: paginatedApplicantsList.body.shortlistedCount,
                  reject: paginatedApplicantsList.body.rejectedCount,
                }
          }
          onStatusChange={(status) => {
            setValue("status", status);
          }}
        />

        <SectionContainer
          h="100%"
          style={{
            overflow: "hidden",
          }}
        >
          <Stack gap={40} py={{ base: 10, sm: 20 }}>
            <JobSeekerApplicantsList
              paginatedApplicantsList={paginatedApplicantsList?.body}
              onPageChange={(pageNumber) =>
                hForm.setValue("pageNumber", pageNumber)
              }
              pageNumber={filterData.pageNumber}
              applicantsLoading={isLoading}
              applicantsError={error}
              showCardActions={
                filterData.status === JobApplicationStatus.Pending
              }
            />
          </Stack>
        </SectionContainer>
      </Box>
    </Box>
  );
};

const JobSeekerApplicantsList = ({
  paginatedApplicantsList,
  applicantsLoading,
  applicantsError,
  onPageChange,
  pageNumber,
  showCardActions,
}: {
  paginatedApplicantsList?: ManageJobseekerJobsPaginatedListType;
  applicantsLoading: boolean;
  applicantsError: {
    status: ErrorHttpStatusCode;
    body: unknown;
    headers: Headers;
  } | null;
  pageNumber: number;
  onPageChange: (val: number) => void;
  showCardActions: boolean;
}) => {
  const showMobileCardView = useMediaQuery("(max-width: 1024px)");
  const { makeApiCall } = useApi();
  const [openedShortlist, { open: onShortlistOpen, close: onShortlistClose }] =
    useDisclosure(false);
  const [openedReject, { open: onRejectOpen, close: onRejectClose }] =
    useDisclosure(false);
  const [
    openedCoverLetter,
    { open: onCoverLetterOpen, close: onCoverLetterClose },
  ] = useDisclosure(false);
  const { showToast } = useCustomToast();
  const [selectedApplicant, setSelectedApplicant] =
    useState<JobSeekerApplicantsResponse | null>(null);
  const [coverLetter, setCoverLetter] = useState<string | null>(null);

  const mutateQueryClient = useQueryClient();

  if (applicantsError) {
    return <ErrorMessage error={applicantsError} />;
  }

  if (applicantsLoading) {
    return <JobListSkeletonCard />;
  }

  if (
    isNil(paginatedApplicantsList) ||
    isEmpty(paginatedApplicantsList.results)
  ) {
    return (
      <Center>
        <Text>Candidates not found</Text>
      </Center>
    );
  }
  const applicants = paginatedApplicantsList.results;

  const handleShortlist = () => {
    if (isNil(selectedApplicant)) {
      showToast({
        id: "shortlist_error",
        message: "Please select an applicant to shortlist",
        status: ToastStatus.warning,
      });
      return;
    }
    makeApiCall({
      fetcherFn: async () => {
        const response =
          await getQueryClient().job.changeJobSeekerApplicationStatus.mutation({
            body: {
              jobId: selectedApplicant.jobId,
              applicantUserId: selectedApplicant.jobSeekerId,
              changedStatus: JobApplicationStatus.Shortlisted,
            },
          });
        return response;
      },
      successMsgProps: {
        message: `${getFullName(
          selectedApplicant.firstName,
          selectedApplicant.lastName
        )} is shortlisted`,
      },
      onSuccessFn: () => {
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.job.manageJobApplicationsOfJobseekers.path],
        });
        handleShortlistClose();
      },
    });
  };

  const handleReject = () => {
    if (isNil(selectedApplicant)) {
      showToast({
        id: "reject_error",
        message: "Please select an applicant to reject",
        status: ToastStatus.warning,
      });
      return;
    }
    makeApiCall({
      fetcherFn: async () => {
        const response =
          await getQueryClient().job.changeJobSeekerApplicationStatus.mutation({
            body: {
              jobId: selectedApplicant.jobId,
              applicantUserId: selectedApplicant.jobSeekerId,
              changedStatus: JobApplicationStatus.Rejected,
            },
          });
        return response;
      },
      successMsgProps: {
        message: `${getFullName(
          selectedApplicant.firstName,
          selectedApplicant.lastName
        )} is rejected`,
      },
      onSuccessFn: () => {
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.job.manageJobApplicationsOfJobseekers.path],
        });
        handleRejectClose();
      },
    });
  };

  const handleShortlistClose = () => {
    onShortlistClose();
    setSelectedApplicant(null);
  };

  const handleRejectClose = () => {
    onRejectClose();
    setSelectedApplicant(null);
  };

  return (
    <>
      <ConfirmationModal
        opened={openedShortlist}
        onClose={handleShortlistClose}
        modalHeader={`Are you sure you want to shortlist${
          selectedApplicant
            ? ` ${selectedApplicant.firstName} ${selectedApplicant.lastName}`
            : ""
        }?`}
        onSuccessFn={handleShortlist}
        successButtonLabel="Yes"
        secondaryButtonLabel="No"
      />
      <ConfirmationModal
        opened={openedReject}
        onClose={handleRejectClose}
        modalHeader={`Are you sure you want to reject${
          selectedApplicant
            ? ` ${selectedApplicant.firstName} ${selectedApplicant.lastName}`
            : ""
        }?`}
        onSuccessFn={handleReject}
        successButtonLabel="Yes"
        secondaryButtonLabel="No"
      />
      <CoverLetterModal
        coverLetter={coverLetter ?? ""}
        opened={openedCoverLetter}
        onClose={() => {
          onCoverLetterClose();
          setCoverLetter(null);
        }}
        modalHeader={"Cover letter of the applicant"}
      />
      {applicants.map((applicant) => (
        <Fragment key={applicant.jobSeekerId}>
          {showMobileCardView ? (
            <JobSeekerApplicantMobileCard
              applicant={applicant}
              onShortlist={() => {
                setSelectedApplicant(applicant);
                onShortlistOpen();
              }}
              onReject={() => {
                setSelectedApplicant(applicant);
                onRejectOpen();
              }}
              showActions={showCardActions}
              onCoverLetterOpen={() => {
                setCoverLetter(applicant.coverLetter);
                onCoverLetterOpen();
              }}
            />
          ) : (
            <JobSeekerApplicantCard
              applicant={applicant}
              onShortlist={() => {
                setSelectedApplicant(applicant);
                onShortlistOpen();
              }}
              onReject={() => {
                setSelectedApplicant(applicant);
                onRejectOpen();
              }}
              showActions={showCardActions}
              onCoverLetterOpen={() => {
                setCoverLetter(applicant.coverLetter);
                onCoverLetterOpen();
              }}
            />
          )}
        </Fragment>
      ))}

      <Center>
        <CustomPagination
          value={pageNumber}
          onChange={onPageChange}
          total={paginatedApplicantsList.totalPages}
        />
      </Center>
    </>
  );
};

export default ManageJobSeekerApplicants;
