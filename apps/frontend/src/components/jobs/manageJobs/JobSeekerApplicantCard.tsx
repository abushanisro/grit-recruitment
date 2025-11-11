import { JobSeekerApplicantsResponse } from "@/types/jobs";
import { Group, Text, Grid, Stack } from "@mantine/core";
import isNil from "lodash/isNil";
import ManageCardActions from "@components/jobs/jobcard/ManageCardActions";
import TextWithImage from "@components/jobs/jobcard/TextWithImage";
import JobCardWithProfile from "@/components/jobs/manageJobs/JobCardWithProfile";
import Link from "next/link";
import { ExperienceTextWithImage } from "@/components/jobs/manageJobs/JobSeekerApplicantMobileCard";
import CustomBadge from "@/components/jobs/CustomBadge";
import { getNoticePeriodLabel, getTimeFromNow } from "@/utils/common";
import isEmpty from "lodash/isEmpty";
import { generateSlugForIdPagePath } from "contract/utils";

const JobSeekerApplicantCard = ({
  onCoverLetterOpen,
  applicant,
  onShortlist,
  onReject,
  showActions,
}: {
  onCoverLetterOpen: () => void;
  applicant: JobSeekerApplicantsResponse;
  onShortlist: () => void;
  onReject: () => void;
  showActions: boolean;
}) => {
  return (
    <JobCardWithProfile image={applicant.jobSeekerProfileImage ?? ""}>
      {applicant.lastLoginTime && (
        <CustomBadge
          label={`Active ${getTimeFromNow(new Date(applicant.lastLoginTime))}`}
          pos="absolute"
          right={{ base: 20, sm: 40 }}
          top={-31}
          px={12}
          py={14}
          style={{
            borderRadius: "10px 10px 0px 0px",
          }}
        />
      )}
      <Grid columns={showActions ? 6 : 4}>
        <Grid.Col span={4}>
          <Link
            href={`/viewJobSeeker/${
              applicant.jobApplicationId
            }/${generateSlugForIdPagePath([
              applicant.firstName,
              applicant.lastName,
              applicant.location,
              `${applicant.experience}${
                applicant.experience === 1
                  ? "-year-experience"
                  : "-years-experience"
              }`,
            ])}`}
            style={{ textDecoration: "none" }}
          >
            <Text
              c="white"
              fw={700}
              fz={{ base: 18, sm: 20, xl: 28 }}
              lh="1.17"
              mb={{ base: 10, sm: 12 }}
              tt="capitalize"
              lineClamp={1}
            >
              {`${applicant.firstName} ${applicant.lastName}`}
            </Text>
          </Link>
          <TextWithImage
            imageUrl="/images/Location.svg"
            text={applicant.location}
          />

          <Text
            fz={22}
            fw="600"
            lh="1.25"
            c="var(--mantine-color-secondaryGreen-1)"
            mt={30}
            tt="capitalize"
            lineClamp={4}
          >
            {applicant.jobSeekerDescription}
          </Text>
          <Group wrap="nowrap" mt={25}>
            {!isNil(applicant.expectedSalaryInLpa) && (
              <TextWithImage
                imageUrl="/images/rupee.svg"
                text={`${applicant.expectedSalaryInLpa} LPA`}
              />
            )}
            <ExperienceTextWithImage experience={applicant.experience} />
            <TextWithImage
              imageUrl="/images/timer.svg"
              text={`${getNoticePeriodLabel(applicant.noticePeriod)}`}
            />
          </Group>
        </Grid.Col>
        <Grid.Col span={2} display={showActions ? "block" : "none"}>
          <Stack align="flex-end" justify="space-between" h="100%">
            <ManageCardActions
              showCoverLetterButton={!isEmpty(applicant.coverLetter)}
              onCoverLetterOpen={onCoverLetterOpen}
              resumeUrl={applicant.resume}
              videoResumeUrl={applicant.videoResume}
              onShortlist={onShortlist}
              onReject={onReject}
            />
          </Stack>
        </Grid.Col>
      </Grid>
    </JobCardWithProfile>
  );
};

export default JobSeekerApplicantCard;
