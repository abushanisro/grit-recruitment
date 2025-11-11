import { useRouter } from "next/router";
import Head from "next/head";
import Navbar from "@components/Navbar";
import PageLayout from "@components/layouts/PageLayout";

import JobSeekerViewIdPageContainer from "@components/profile/JobSeekerViewIdPageContainer";

const JobSeekerViewIdPage = () => {
  const router = useRouter();
  const id = String(router.query.id ?? "");

  return (
    <>
      <Head>
        <title>JobseekerProfile | Jobverse</title>
        <meta
          name="description"
          content="Sales Jobverse | Jobseeker Profile "
        />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        pageBg="primaryBackground.9"
        navbarComponent={<Navbar />}
        mainComponent={id ? <JobSeekerViewIdPageContainer id={id} /> : <></>}
      />
    </>
  );
};

export default JobSeekerViewIdPage;
