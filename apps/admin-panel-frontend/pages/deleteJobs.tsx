import DeleteJobs from "@components/DeleteJobs";
import Navbar from "@components/Navbar";
import PageLayout from "@components/layouts/PageLayout";
import Head from "next/head";

const DeleteJobsPage = () => {
  return (
    <>
      <Head>
        <title>Sales Jobverse Admin Panel | Delete Jobs</title>
        <meta name="description" content="Sales Jobverse Admin Panel" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout navbarComponent={<Navbar />} mainComponent={<DeleteJobs />} />
    </>
  );
};

export default DeleteJobsPage;
