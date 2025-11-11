import { useRouter } from "next/router";
import { Avatar, Paper } from "@mantine/core";
import Head from "next/head";
import Navbar from "@components/Navbar";
import PageLayout from "@components/layouts/PageLayout";
import CustomBadge from "@components/jobs/CustomBadge";
import { getQueryClient } from "api";
import { getTimeFromNow } from "@/utils/common";
import CustomSkeleton from "@components/CustomSkeleton";
import JobIdHeader from "@components/jobs/jobId/JobIdHeader";
import JobIdBody from "@components/jobs/jobId/JobIdBody";
import { contract } from "contract";
import { useMediaQuery } from "@mantine/hooks";
import ErrorMessage from "@components/ErrorMessage";

const JobsIdPage = () => {
  const router = useRouter();
  const id = String(router.query.id ?? "");
  return (
    <>
      <Head>
        <title>Jobs | Sales Jobverse</title>
        <meta name="description" content="Jobs | Sales Jobverse" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        pageBg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
        navbarComponent={<Navbar />}
        mainComponent={<JobsIdPageComponent id={id} />}
      />
    </>
  );
};

const JobsIdPageComponent = ({ id }: { id: string }) => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  const { data, isLoading, error } = getQueryClient().job.getJobById.useQuery(
    [contract.job.getJobById.path, id],
    { query: { jobId: id } },
    {
      retry: (failureCount, error) => {
        return error.status >= 500 && error.status < 600 && failureCount < 3;
      },
    }
  );

  if (isLoading) {
    return (
      <CustomSkeleton
        height={600}
        radius="xl"
        mx="auto"
        maw={{ base: 1037, xl: 1037 }}
        mt={{ base: 40, xl: 100 }}
      />
    );
  }
  if (error) {
    return <ErrorMessage error={error} />;
  }

  return (
    <Paper
      pos="relative"
      maw={{ base: 370, xs: 600, sm: 700, md: 800, xl: 1037 }}
      bg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
      style={{
        border: "1px solid var(--mantine-color-secondaryGreen-1)",
        boxShadow: "0px 4px 4px 0px var(--mantine-color-secondaryGreen-1)",
        borderRadius: isMobile ? 10 : 38,
      }}
      ml={{ base: 0, sm: 45 }}
      mt={{ base: 40, sm: 100 }}
    >
      {data.body.adminApprovedTime && (
        <CustomBadge
          label={`posted ${getTimeFromNow(data.body.adminApprovedTime)}`}
          pos="absolute"
          left={{ base: 20, sm: 40 }}
          top={-31}
          px={12}
          py={14}
          style={{
            borderRadius: "10px 10px 0px 0px",
          }}
        />
      )}
      <Avatar
        pos="absolute"
        w={{ base: 60, md: 80, xl: 120 }}
        h={{ base: 60, md: 80, xl: 120 }}
        left={{ base: "20px", sm: "-45px" }}
        top={{ base: "75px", sm: "240px", md: "240px", xl: "210px" }}
        bg="customGray.2"
        style={{
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryGreen-1)",
        }}
        src={data.body.companyLogo ?? ""}
        alt="company logo"
      />
      <JobIdHeader id={id} data={data} />
      <JobIdBody data={data} />
    </Paper>
  );
};

export default JobsIdPage;
