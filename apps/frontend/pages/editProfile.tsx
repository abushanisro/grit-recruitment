import Head from "next/head";
import PageLayout from "@components/layouts/PageLayout";
import Navbar from "@components/Navbar";
import { UserRole } from "contract/enum";
import { useUserData } from "@/contexts/UserProvider";
import EditJobSeekerProfilePageComponent from "@components/profile/jobSeeker/EditJobSeekerProfilePageComponent";
import EditEmployerProfilePageComponent from "@components/profile/employer/EditEmployerProfilePageComponent";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";

const EditProfilePage = () => {
  return (
    <>
      <Head>
        <title>Edit Profile | Sales Jobverse</title>
        <meta name="description" content="Sales Jobverse - Edit profile" />
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
      return <EditJobSeekerProfilePageComponent />;
    case UserRole.employer:
      return <EditEmployerProfilePageComponent />;
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

export default EditProfilePage;
