import PlayVideoResumeComponent from "@components/PlayVideoResumeComponent";

import Head from "next/head";

const PlayVideoResumePage = () => {
  return (
    <>
      <Head>
        <title>Play video Resume | Sales Jobverse</title>
        <meta name="description" content="Play video resume" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PlayVideoResumeComponent />
    </>
  );
};
export default PlayVideoResumePage;
