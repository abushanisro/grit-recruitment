import Navbar from "@components/Navbar";
import PageLayout from "@components/layouts/PageLayout";
import TermsAndConditionPage from "@components/termsAndCondition/TermsAndConditionPage";
import Head from "next/head";

const TermsAndCondition = () => {
  return (
    <>
      <Head>
        <title>Terms and condition | Sales Jobverse</title>
        <meta
          name="description"
          content="Sales Jobverse - Terms and condition"
        />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        navbarComponent={<Navbar />}
        mainComponent={<TermsAndConditionPage />}
      />
    </>
  );
};
export default TermsAndCondition;
