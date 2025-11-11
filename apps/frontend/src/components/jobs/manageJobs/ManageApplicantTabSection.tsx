import { ManageJobApplicantCountInterface } from "@/types/jobs";
import { ActiveTableTabButton } from "@components/buttons/ActiveTableTabButton";
import { Group } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { JobApplicationStatus } from "contract/enum";
import { Fragment } from "react";

const getStatusLabel = (
  status: JobApplicationStatus,
  counts: ManageJobApplicantCountInterface | null
): string => {
  switch (status) {
    case JobApplicationStatus.Pending:
      return `Pending ${counts ? `(${counts.pending})` : ""}`;
    case JobApplicationStatus.Rejected:
      return `Rejected ${counts ? `(${counts.reject})` : ""}`;
    case JobApplicationStatus.Shortlisted:
      return `Shortlisted ${counts ? `(${counts.shortlist})` : ""}`;
    default:
      return "";
  }
};

const ManageJobApplicantTabSection = ({
  selectedStatus,
  onStatusChange,
  countData,
}: {
  selectedStatus: string;
  onStatusChange: (val: JobApplicationStatus) => void;
  countData: ManageJobApplicantCountInterface | null;
}) => {
  const isTablet = useMediaQuery("(max-width: 767px)");

  return (
    <Group
      justify="flex-start"
      wrap="nowrap"
      pl={{ base: 0, sm: 40, md: 80 }}
      mt={{ base: 12, md: 24 }}
      pb={isTablet ? 16 : 0}
      style={{
        overflowX: "scroll",
      }}
    >
      {Object.values(JobApplicationStatus).map((applicationStatus, index) => {
        const isActive = selectedStatus === applicationStatus;
        return (
          <Fragment key={index}>
            <ActiveTableTabButton
              isActive={isActive}
              label={getStatusLabel(applicationStatus, countData)}
              onClick={() => {
                onStatusChange(applicationStatus);
              }}
            />
          </Fragment>
        );
      })}
    </Group>
  );
};

export default ManageJobApplicantTabSection;
