import Head from "next/head";
import PageLayout from "@components/layouts/PageLayout";
import Navbar from "@components/Navbar";
import { useUserData } from "@/contexts/UserProvider";
import { UserRole } from "contract/enum";
import JobSeekerProfileComponent from "@components/profile/jobSeeker/JobSeekerProfileComponent";
import EmployerProfilePageComponent from "@components/profile/employer/EmployerProfileComponent";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";

const ProfilePage = () => {
  return (
    <>
      <Head>
        <title>Sales Jobverse | Profile</title>
        <meta name="description" content="Sales Jobverse | Profile" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        navbarComponent={<Navbar />}
        mainComponent={<RenderProfilePageComponent />}
      />
    </>
  );
};

const renderProfileComponent = (role: UserRole) => {
  switch (role) {
    case UserRole.jobSeeker:
      return <JobSeekerProfileComponent />;
    case UserRole.employer:
      return <EmployerProfilePageComponent />;
    case UserRole.admin:
    default:
      return <></>;
  }
};
const RenderProfilePageComponent = () => {
  const { userDetails } = useUserData();
  if (!userDetails) {
    return <CustomErrorMessage errorMessage="User data not available" />;
  }
  return <>{renderProfileComponent(userDetails.role)}</>;
};

export default ProfilePage;
