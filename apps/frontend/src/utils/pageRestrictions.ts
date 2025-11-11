export const noCheckPagePatterns = [
  /\/profile\//,
  /\/editProfile\//,
  /\/termsAndCondition\//,
  /\/about\//,
  /\/jobs\/.*/,
];

export const jobSeekerPagePatterns = [
  /\/job\/.*/,
  /\/jobs\/.*/,
  /\/myJobs\/.*/,
];

export const employerPagePatterns = [
  /\/viewJobSeeker\/.*/,
  /\/employer\/.*/,
  /\/manageJobs\/.*/,
  /\/manageJobSeekerApplicants\/.*/,
  /\/postJob\/.*/,
  /\/playVideoResume\/.*/,
  /\/jobseekerDirectoryFilter\/.*/,
  /\/jobseekerDirectory\/.*/,
  /\/jobSeekerThankYou\/.*/,
  /\/jobseeker\/.*/,
  /\/thankYou\/.*/,
  /\/subscription\/.*/,
  /\/subscriptionDetails\/.*/,
  /\/payment\//,
  /\/postPaidPlans\/.*/,
  /\/postPaidJobPlans\/.*/,
  /\/paidJobThankYou\/.*/,
];

export const isPageMatchingAnyPattern = ({
  currentPageAsPath,
  pagePatterns,
}: {
  currentPageAsPath: string;
  pagePatterns: RegExp[];
}) => {
  return pagePatterns
    .map((pattern) => pattern.test(currentPageAsPath))
    .some((match) => match);
};
