import EmployerHomePage from "@components/employer/EmployerHomePage";
import PageLayout from "@components/layouts/PageLayout";
import Navbar from "@components/Navbar";
import Head from "next/head";

const Employer = () => {
  return (
    <>
      <Head>
        <title>Employer | Sales Jobverse</title>
        <meta name="description" content="Sales Jobverse - Employer" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        navbarComponent={<Navbar />}
        mainComponent={<EmployerHomePage />}
      />
    </>
  );
};

export default Employer;
