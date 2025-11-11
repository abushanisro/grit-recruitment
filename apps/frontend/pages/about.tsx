import Navbar from "@components/Navbar";
import AboutPage from "@components/about/AboutPage";
import PageLayout from "@components/layouts/PageLayout";
import Head from "next/head";

const About = () => {
  return (
    <>
      <Head>
        <title>About | Sales Jobverse</title>
        <meta name="description" content="Sales Jobverse - About" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout navbarComponent={<Navbar />} mainComponent={<AboutPage />} />
    </>
  );
};
export default About;
