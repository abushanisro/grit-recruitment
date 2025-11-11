import CustomBadge from "@components/jobs/CustomBadge";
import { Image, Paper, Grid, Flex, Box, Tooltip } from "@mantine/core";
import IconWithLabel from "@components/jobs/jobcard/IconWithLabel";
import { JobIdResultsType } from "@/types/jobs";
import isEmpty from "lodash/isEmpty";

const AdditionalDetails = ({ data }: { data: { body: JobIdResultsType } }) => {
  const salaryRange = [
    data.body.minSalaryInlpa ?? "",
    data.body.maxSalaryInLpa ?? "",
  ].filter((item) => item !== "");
  return (
    <Grid.Col span={{ base: 12, md: 5, xl: 4 }}>
      <Flex
        align={{ base: "flex-start", md: "flex-end" }}
        direction="column"
        gap={10}
      >
        {!isEmpty(data.body.employmentModes) && (
          <Tooltip label={data.body.employmentModes.join(", ")}>
            <CustomBadge
              maw={200}
              style={{ textOverflow: "ellipsis" }}
              label={data.body.employmentModes.join(", ")}
              c="secondaryGreen.1"
              fz={{ base: 14, xl: 16 }}
              px={12}
              py={12}
              bg="primaryGrey.1"
            />
          </Tooltip>
        )}
        <Paper
          bg="primaryGrey.1"
          style={{
            borderRadius: 30,
          }}
          p={{ base: 30, sm: 40 }}
        >
          {!isEmpty(data.body.locations) && (
            <IconWithLabel
              icon={
                <Box miw={26}>
                  <Image
                    src="/images/location.svg"
                    alt="location"
                    w={20}
                    h={26}
                  />{" "}
                </Box>
              }
              label={data.body.locations
                .map((eachLocation) => eachLocation.name)
                .join(", ")}
              gap={10}
              mb={30}
              fontProps={{ maw: 100 }}
            />
          )}
          {!isEmpty(data.body.employmentTypes) && (
            <IconWithLabel
              icon={
                <Box miw={26}>
                  <Image src="/images/clock.svg" alt="time" w={24} h={24} />
                </Box>
              }
              label={data.body.employmentTypes
                .map((eachType) => eachType)
                .join(", ")}
              gap={10}
              mb={30}
              fontProps={{ maw: 100 }}
            />
          )}

          {!isEmpty(salaryRange) && (
            <IconWithLabel
              icon={
                <Box miw={26}>
                  <Image src="/images/moneyBag.svg" alt="money" w={24} h={24} />
                </Box>
              }
              label={`${salaryRange.join("-")} LPA`}
              gap={10}
            />
          )}
        </Paper>
      </Flex>
    </Grid.Col>
  );
};
export default AdditionalDetails;
