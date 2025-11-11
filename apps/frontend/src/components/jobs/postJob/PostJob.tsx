import CustomSkeleton from "@components/CustomSkeleton";
import { contract } from "contract";
import { getQueryClient } from "api";
import ErrorMessage from "@components/ErrorMessage";
import CustomErrorMessage from "@/components/jobs/CustomErrorMessage";
import JobSeekerJobForm from "@/components/jobs/postJob/JobSeekerJobForm";

const PostJob = () => {
  const { data, isLoading, error } =
    getQueryClient().user.getEmployerById.useQuery(
      [contract.user.getEmployerById.path],
      {}
    );
  if (isLoading) {
    return (
      <CustomSkeleton
        height={600}
        radius="xl"
        mx="auto"
        maw={{ base: 1037, xl: 1037 }}
        mt={{ base: 40, xl: 100 }}
      />
    );
  }
  if (error) {
    return <ErrorMessage error={error} />;
  }
  if (data?.status !== 200) {
    return <CustomErrorMessage errorMessage="User data not available" />;
  }

  return <JobSeekerJobForm data={data.body} />;
};
export default PostJob;
