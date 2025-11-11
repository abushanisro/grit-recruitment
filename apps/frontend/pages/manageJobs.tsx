import Head from "next/head";
import PageLayout from "@components/layouts/PageLayout";
import Navbar from "@components/Navbar";
import { useUserData } from "@/contexts/UserProvider";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import ManageJobs from "@components/jobs/manageJobs/ManageJobs";

const ManageJobsPage = () => {
  return (
    <>
      <Head>
        <title>Manage Jobs | Sales Jobverse</title>
        <meta name="description" content="Sales Jobverse | Manage Jobs" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        navbarComponent={<Navbar />}
        mainComponent={<ManageJobsPageComponent />}
      />
    </>
  );
};
const ManageJobsPageComponent = () => {
  const { userDetails } = useUserData();
  if (!userDetails) {
    return <CustomErrorMessage errorMessage="User data not available" />;
  }
  return <ManageJobs />;
};
export default ManageJobsPage;
