import Head from "next/head";
import Navbar from "@components/Navbar";
import JobsOtherFilters from "@components/jobs/filters/JobsOtherFilter";
import JobSearchField from "@components/jobs/filters/JobSearchField";
import { useForm, UseFormReturn } from "react-hook-form";
import PageLayout from "@components/layouts/PageLayout";
import { Box, Group } from "@mantine/core";
import JobCardsList from "@components/jobs/JobCardsList";
import { useQueryState } from "@/hooks/queryState";
import { useEffect } from "react";
import { useRouter } from "next/router";
import { FilterInterface } from "@/types/jobs";

const JobsPage = () => {
  const router = useRouter();
  const defaultValueObj = {
    searchText: "",
    pageNumber: 1,
    pageSize: 20,
    location: null,
    employmentType: null,
    salary: null,
    workSchedule: null,
    industry: null,
    experience: null,
    subFunctions: null,
  };
  const [selectedFilters, setSelectedFilters] = useQueryState<FilterInterface>(
    "jobsPageFilter",
    defaultValueObj
  );

  const hForm = useForm<FilterInterface>({
    mode: "onChange",
    defaultValues: selectedFilters,
  });
  const { reset, watch, setValue } = hForm;

  //NOTE: TODO need to fix fetching and applying of filter state
  useEffect(() => {
    reset(selectedFilters);
  }, [router.isReady]);

  const formData = watch();
  useEffect(() => {
    if (!router.isReady) {
      return;
    }
    const { pageNumber, ...otherFilters } = selectedFilters;

    const otherFiltersChanged = Object.keys(otherFilters).some((key) => {
      const inferedKey = key as keyof Omit<FilterInterface, "pageNumber">;
      return (
        JSON.stringify(formData[inferedKey]) !==
        JSON.stringify(selectedFilters[inferedKey])
      );
    });

    if (otherFiltersChanged) {
      setSelectedFilters({ ...formData, pageNumber: 1 });
      setValue("pageNumber", 1);
    } else if (formData.pageNumber !== selectedFilters.pageNumber) {
      setSelectedFilters(formData);
    }
  }, [router.isReady, JSON.stringify(formData)]);

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
        navbarComponent={
          <Navbar>
            <>
              <JobSearchField
                name="searchText"
                hForm={hForm}
                onReset={() => setValue("searchText", "")}
              />
              <JobsOtherFilters hForm={hForm} />
            </>
          </Navbar>
        }
        mainComponent={<JobsPageComponent hForm={hForm} />}
      />
    </>
  );
};

const JobsPageComponent = ({
  hForm,
}: {
  hForm: UseFormReturn<FilterInterface, any, undefined>;
}) => {
  const { setValue } = hForm;
  return (
    <Box>
      <Box display={{ base: "none", sm: "block" }} py={20} w="100%">
        <Box maw={{ base: 300, sm: 600, md: 800, lg: 1100, xl: 1200 }}>
          <JobSearchField
            name="searchText"
            hForm={hForm}
            onReset={() => setValue("searchText", "")}
          />
          <Group
            mt={20}
            maw={{ base: "100%", md: "100%", lg: "80%" }}
            style={{
              alignItems: "flex-start",
            }}
          >
            <JobsOtherFilters hForm={hForm} />
          </Group>
        </Box>
      </Box>
      <JobCardsList hForm={hForm} />
    </Box>
  );
};

export default JobsPage;
