import Head from "next/head";

import PageLayout from "@components/layouts/PageLayout";

import ThankYouComponent from "@components/thankYou/ThankYouComponent";

const ThankYouPage = () => {
  return (
    <>
      <Head>
        <title>Thank you</title>
        <meta name="description" content="Sales Jobverse - Thank you" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        pageBg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
        mainComponent={<ThankYouComponent />}
      />
    </>
  );
};

export default ThankYouPage;
