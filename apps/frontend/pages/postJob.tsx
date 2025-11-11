import Head from "next/head";
import PageLayout from "@components/layouts/PageLayout";
import Navbar from "@components/Navbar";
import { useUserData } from "@/contexts/UserProvider";
import CustomErrorMessage from "@components/jobs/CustomErrorMessage";
import PostJob from "@components/jobs/postJob/PostJob";

const PostJobPage = () => {
  return (
    <>
      <Head>
        <title>Post Job | Sales Jobverse</title>
        <meta name="description" content="Sales Jobverse - Post Job" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        navbarComponent={<Navbar />}
        mainComponent={<PostJobPageComponent />}
      />
    </>
  );
};

const PostJobPageComponent = () => {
  const { userDetails } = useUserData();
  if (!userDetails) {
    return <CustomErrorMessage errorMessage="User data not available" />;
  }
  return <PostJob />;
};

export default PostJobPage;
