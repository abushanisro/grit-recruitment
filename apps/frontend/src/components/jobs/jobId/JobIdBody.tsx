import { Group, Paper, Text, Divider, Grid, Tooltip } from "@mantine/core";
import { getColor } from "@/utils/colors";
import { Fragment } from "react";
import AdditionalDetails from "@components/jobs/jobId/body/AdditionalDetails";
import JobIdBodyDetailedDescription from "@components/jobs/jobId/body/DetailedDescription";
import { JobIdResultsType } from "@/types/jobs";
import SpacedOutText from "@/components/jobs/jobId/SpacedOutText";
import isEmpty from "lodash/isEmpty";

const JobIdBody = ({ data }: { data: { body: JobIdResultsType } }) => {
  const attributesData = [
    !isEmpty(data.body.industries)
      ? data.body.industries.map((eachIndustry) => eachIndustry.name).join(", ")
      : "",
    !isEmpty(data.body.subFunctions)
      ? data.body.subFunctions
          .map((eachFunction) => eachFunction.name)
          .join(", ")
      : "",
  ].filter((item) => item !== "");
  return (
    <Paper
      pl={{ base: 30, sm: 50, md: 100, xl: 131 }}
      pt={20}
      pb={54}
      pr={30}
      ta={"justify"}
      
      bg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
      style={{
        borderTopLeftRadius: 0,
        borderTopRightRadius: 0,
        borderBottomLeftRadius: 38,
        borderBottomRightRadius: 38,
       
      }}
    >
      <SpacedOutText label={data.body.companyName} />
      <Group mb={{ base: 40, md: 50, xl: 70 }}>
        {attributesData.map((label: string, index: number) => {
          return (
            <Fragment key={index}>
              <Tooltip label={label}>
                <Text
                  tt="uppercase"
                  c={getColor(index)}
                  fw="400"
                  lh={{ base: 1, sm: "1.17" }}
                  fz={{ base: 16, sm: 18, xl: 23 }}
                  mb={{ base: 6, sm: 33 }}
                  maw={{ base: 200, sm: 300 }}
                  truncate
                >
                  {label}
                </Text>
              </Tooltip>
              {attributesData.length - 1 !== index && (
                <Divider orientation="vertical" h={20} />
              )}
            </Fragment>
          );
        })}
      </Group>
      <Grid>
        <JobIdBodyDetailedDescription data={data} />
        <AdditionalDetails data={data} />
      </Grid>
    </Paper>
  );
};

export default JobIdBody;
