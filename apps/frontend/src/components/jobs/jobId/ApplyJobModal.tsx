import CustomModal from "@/components/CustomModal";
import { useState } from "react";
import { useForm, SubmitHandler } from "react-hook-form";
import { Box, Text, Avatar, Flex, Divider, Stack, Grid } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import YellowText from "@/components/YellowText";
import CustomTextArea from "@components/form/CustomTextArea";
import { useApi } from "@/hooks/useApi";
import { getQueryClient } from "api";
import { contract } from "contract";
import { useQueryClient } from "@tanstack/react-query";
import CustomRadioBox from "@components/form/CustomRadioBox";
import { yesOrNoEnum } from "@/types/jobSeeker";
import isEmpty from "lodash/isEmpty";
import { IconX } from "@tabler/icons-react";
import JobApplyButton from "@components/jobs/JobApplyButton";

const options = Object.values(yesOrNoEnum).map((value) => ({
  label: value,
  value: value,
}));
interface ApplyJobInterface {
  okayWithLocation: null | yesOrNoEnum;
  coverLetter: string | null;
}
const ApplyJobModal = ({
  opened,
  onClose,
  title,
  designation,
  logoUrl,
  jobId,
}: {
  opened: boolean;
  onClose: () => void;
  title: string;
  designation: string;
  logoUrl: string | null;
  jobId: string;
}) => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  const isTablet = useMediaQuery("(max-width: 768px)");

  const hForm = useForm<ApplyJobInterface>({
    mode: "onChange",
    defaultValues: {
      okayWithLocation: null,
      coverLetter: null,
    },
  });
  const { handleSubmit } = hForm;
  const mutateQueryClient = useQueryClient();
  const { makeApiCall } = useApi();
  const [loader, setLoader] = useState<boolean>(false);
  const onSubmit: SubmitHandler<ApplyJobInterface> = (data) => {
    setLoader(true);
    makeApiCall({
      fetcherFn: async () => {
        const response = await getQueryClient().job.applyJob.mutation({
          body: {
            jobId: Number(jobId),
            areYouOkayWithTheLocation:
              data.okayWithLocation === yesOrNoEnum.yes,
            coverLetter: isEmpty(data.coverLetter) ? null : data.coverLetter,
          },
        });
        return response;
      },

      successMsgProps: { message: "Applied to job successfully" },
      onSuccessFn: () => {
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.job.getJobById.path],
        });
        setLoader(false);
        onClose();
      },
      finallyFn: () => {
        setLoader(false);
      },
      showFailureMsg: true,
    });
  };

  const checkNameLength = (value: string | null) => {
    if (!value) {
      return true;
    }
    if (value.length < 20 || value.length > 2000) {
      return "Cover Letter must be between 20 and 2000 characters.";
    }
    return true; // Validation passed
  };

  const checkRadioButton = (value: yesOrNoEnum) => {
    if (!value || null) {
      return "This field is Required";
    }

    return true; // Validation passed
  };

  return (
    <CustomModal
      opened={opened}
      onClose={onClose}
      closeOnClickOutside={true}
      closeOnEscape={true}
      size={isTablet ? "100%" : "max-content"}
    >
      <Box
        pos="absolute"
        top={15}
        right={15}
        style={{
          cursor: "pointer",
          zIndex: 100,
        }}
        c="white"
        onClick={onClose}
      >
        <IconX width={isMobile ? 24 : 30} height={isMobile ? 24 : 30} />
      </Box>
      <form onSubmit={handleSubmit(onSubmit)}>
        <Stack
          gap={25}
          miw={{ base: "100%", sm: 500, lg: 700, xl: 800 }}
          px={{ base: 0, md: 24, lg: 30, xl: 30 }}
          py={10}
          style={{ position: "relative" }}
        >
          <Grid gutter={{ base: 10, lg: 46 }}>
            <Grid.Col span="content">
              <Avatar
                src={logoUrl}
                style={{
                  cursor: "pointer",
                  border: "2px solid",
                  borderColor: "var(--mantine-color-secondaryGreen-1)",
                  borderRadius: "50%",
                  background: "white",
                }}
                w={{ base: 60, md: 90, lg: 120, xl: 120 }}
                h={{ base: 60, md: 90, lg: 120, xl: 120 }}
                alt="company logo"
              />
            </Grid.Col>
            <Grid.Col span="auto">
              <Text
                c="white"
                fw={{ lg: "400" }}
                fz={{ base: 18, lg: 33.48, xl: 33.48 }}
                mt={{ base: 5, md: 10, lg: 10, xl: 10 }}
              >
                {title}
              </Text>
              <Text
                mt={4}
                c="white"
                fw={{ lg: "700" }}
                fz={{ base: 12, lg: 22 }}
                style={{ letterSpacing: "6.68px" }}
              >
                {designation}
              </Text>
            </Grid.Col>
          </Grid>

          <Flex
            gap={{ sm: 5, md: 15, lg: 20, xl: 20 }}
            justify="flex-start"
            align="flex-start"
            direction={{ base: "column", md: "row", xl: "row" }}
            wrap="wrap"
            mt="20"
          >
            <YellowText
              label="Are you okay with the location?"
              fz={16}
              pb={16}
              fw="700"
            />

            <Divider
              display={{ base: "none", md: "block" }}
              w={2}
              h={24}
              orientation="vertical"
              color="secondaryGreen.1"
            />

            <CustomRadioBox
              hForm={hForm}
              name="okayWithLocation"
              rules={{
                validate: {
                  checkRadioButton: (v) => checkRadioButton(v),
                },
              }}
              options={options}
            />
          </Flex>
          <Box>
            <YellowText
              label=" Cover Letter (optional)"
              fz={16}
              pb={16}
              fw="700"
            />
            <CustomTextArea
              hForm={hForm}
              name="coverLetter"
              styles={{
                label: {
                  color: "var(--mantine-color-secondaryGreen-1)",
                  fontWeight: 700,
                  fontSize: 17,
                  paddingBottom: 10,
                },

                input: {
                  color: "white",
                  background: "black",
                  borderRadius: 9,
                  height: 150,
                  paddingLeft: isMobile ? 12 : 20,
                  paddingRight: isMobile ? 12 : 30,
                  paddingBlock: 20,
                },
              }}
              placeholder="A cover letter is a key tool to impress the employer and convince them you're the ideal candidate for the job."
              minRows={2}
              maxRows={4}
              fz={{ base: 14, md: 14, lg: 24, xl: 24 }}
              rules={{
                validate: {
                  checkNameLength: (v) => checkNameLength(v),
                },
              }}
            />
          </Box>
          <JobApplyButton
            label="Apply Now"
            pos="static"
            w="100%"
            maw={{ base: "100%", md: "max-content" }}
            fz={{ base: 16, sm: 20 }}
            ml={0}
            fw="600"
            px={20}
            py={12}
            mt={15}
            h="max-content"
            type="submit"
            loading={loader}
          />
        </Stack>
      </form>
    </CustomModal>
  );
};

export default ApplyJobModal;
