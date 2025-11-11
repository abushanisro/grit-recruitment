import Head from "next/head";

import PageLayout from "@components/layouts/PageLayout";
import Navbar from "@components/Navbar";

import JobSeekerFormPostPaidPlan from "@components/subscriptionPlan/JobSeekerFormPostPaidPlan";

const PostPaidPlan = () => {
  return (
    <>
      <Head>
        <title>Subscription</title>
        <meta name="description" content="Sales Jobverse - Subscription" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        pageBg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
        navbarComponent={<Navbar />}
        mainComponent={<JobSeekerFormPostPaidPlan />}
      />
    </>
  );
};

export default PostPaidPlan;
