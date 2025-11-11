import { useRouter } from "next/router";
import Head from "next/head";
import Navbar from "@components/Navbar";
import PageLayout from "@components/layouts/PageLayout";
import { getQueryClient } from "api";
import { contract } from "contract";
import ProfileLoader from "@components/loaders/ProfileLoader";
import ErrorMessage from "@components/ErrorMessage";
import JobSeekerProfile from "@components/profile/JobSeekerViewIdPage";

const ViewJobSeekerProfileIdPage = () => {
  const router = useRouter();
  const id = String(router.query.id ?? "");
  return (
    <>
      <Head>
        <title>Jobseeker | Sales Jobverse</title>
        <meta name="description" content="Sales Jobverse | Jobseeker " />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        pageBg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
        navbarComponent={<Navbar />}
        mainComponent={
          id ? <ViewFreelancerProfileIdPageContainer id={id} /> : <></>
        }
      />
    </>
  );
};

const ViewFreelancerProfileIdPageContainer = ({ id }: { id: string }) => {
  const { data, isLoading, error } =
    getQueryClient().user.viewJobseekerProfileFromManageApplication.useQuery(
      [contract.user.viewJobseekerProfileFromManageApplication.path, id],
      { query: { jobApplicationId: id } },
      {
        retry: (failureCount, error) => {
          return error.status >= 500 && error.status < 600 && failureCount < 3;
        },
      }
    );
  if (isLoading) {
    return <ProfileLoader />;
  }
  if (error) {
    return <ErrorMessage error={error} />;
  }
  return <JobSeekerProfile data={data.body} />;
};

export default ViewJobSeekerProfileIdPage;
