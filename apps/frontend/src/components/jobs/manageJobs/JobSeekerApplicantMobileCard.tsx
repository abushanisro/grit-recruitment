import { JobSeekerApplicantsResponse } from "@/types/jobs";
import { Group, Text, Stack, Avatar } from "@mantine/core";
import isNil from "lodash/isNil";
import ManageCardActions from "@components/jobs/jobcard/ManageCardActions";
import TextWithImage from "@components/jobs/jobcard/TextWithImage";
import JobCardWithProfile from "./JobCardWithProfile";
import Link from "next/link";
import CustomBadge from "@/components/jobs/CustomBadge";
import { getNoticePeriodLabel, getTimeFromNow } from "@/utils/common";
import isEmpty from "lodash/isEmpty";
import { generateSlugForIdPagePath } from "contract/utils";

export const ExperienceTextWithImage = ({
  experience,
}: {
  experience: number;
}) => {
  return (
    <TextWithImage
      imageUrl="/images/clock.svg"
      text={
        experience === 0
          ? "Fresher"
          : experience > 1
          ? `${experience} years`
          : `${experience} year`
      }
    />
  );
};

const JobSeekerApplicantMobileCard = ({
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
    <JobCardWithProfile showImage={false}>
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
      <Group>
        <Avatar
          w={70}
          h={70}
          src={applicant.jobSeekerProfileImage}
          bg="customGray.2"
          style={{
            border: "1px solid",
            borderColor: "var(--mantine-color-primaryGreen-1)",
          }}
          alt="profile pic"
        />
        <Stack gap={10}>
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
              fz={{ base: 20, xl: 28 }}
              lh="1.17"
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
        </Stack>
      </Group>
      <Text
        fz={{ base: 17, md: 20 }}
        fw="600"
        lh="1.25"
        c="var(--mantine-color-secondaryGreen-1)"
        mt={20}
        tt="capitalize"
        lineClamp={4}
      >
        {applicant.jobSeekerDescription}
      </Text>
      <Group mt={20}>
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
      <Group
        align="flex-end"
        justify="space-between"
        h="100%"
        mt={20}
        display={showActions ? "flex" : "none"}
      >
        <ManageCardActions
          showCoverLetterButton={!isEmpty(applicant.coverLetter)}
          resumeUrl={applicant.resume}
          videoResumeUrl={applicant.videoResume}
          onShortlist={onShortlist}
          onReject={onReject}
          onCoverLetterOpen={onCoverLetterOpen}
        />
      </Group>
    </JobCardWithProfile>
  );
};

export default JobSeekerApplicantMobileCard;
