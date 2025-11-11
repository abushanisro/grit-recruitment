import Navbar from "@components/Navbar";
import PageLayout from "@components/layouts/PageLayout";
import UploadJobs from "@components/uploadJobs";
import Head from "next/head";

const UploadJobsPage = () => {
  return (
    <>
      <Head>
        <title>Sales Jobverse Admin Panel | Upload Jobs</title>
        <meta name="description" content="Sales Jobverse Admin Panel" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout navbarComponent={<Navbar />} mainComponent={<UploadJobs />} />
    </>
  );
};

export default UploadJobsPage;
