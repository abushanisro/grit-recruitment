import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { Box, Text } from "@mantine/core";
import { useForm } from "react-hook-form";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { DeleteJobInterface } from "@/types/job";
import CustomInputField from "@/components/form/CustomInputField";
import { useApi } from "@/hooks/useApi";
import { getQueryClient } from "api";

export const deleteFileToastId = "deleteFileToastId";
const defaultFormValues: DeleteJobInterface = {
  id: "",
};
const DeleteJobs = () => {
  const hForm = useForm<DeleteJobInterface>({
    mode: "onChange",
    defaultValues: defaultFormValues,
  });
  const { handleSubmit } = hForm;
  const { showToast } = useCustomToast();
  const { makeApiCall } = useApi();
  const handleDeleteJob = (payload: DeleteJobInterface) => {
    const jobId = payload.id;
    if (!jobId) {
      showToast({
        status: ToastStatus.error,
        id: deleteFileToastId,
        message: "Job ID is required",
      });
      return;
    }
    makeApiCall({
      fetcherFn: async () => {
        const response =
          await getQueryClient().job.deleteJobseekerJobAsAdmin.mutation({
            body: { jobId: Number(jobId) },
          });
        return response;
      },

      successMsgProps: { message: "Job deleted successfully." },
      onSuccessFn: () => {
        hForm.setValue("id", "");
      },
      showFailureMsg: true,
    });
  };
  return (
    <Box mt={80}>
      <Text fz={{ base: 20, sm: 24 }} fw={600} lh="1.16" mb={20} ta="center">
        Delete jobs for jobseekers
      </Text>
      <form onSubmit={handleSubmit(handleDeleteJob)}>
        <Box
          bg="secondaryRed.9"
          p={30}
          style={{ borderRadius: 20 }}
          my={30}
          mx="auto"
        >
          <Box w="100%" my={20}>
            <CustomInputField
              hForm={hForm}
              name="id"
              label="Enter Job ID to delete"
              placeholder="Job ID"
              rules={{
                required: "Job ID is required",
                validate: {
                  checkType: (value) => {
                    if (isNaN(value)) {
                      return "Job ID must be a number.";
                    }
                    return true; // Validation passed
                  },
                },
              }}
            />
          </Box>
          <Box ta="center">
            <PrimaryButton label="Delete Job" type="submit" />
          </Box>
        </Box>
      </form>
    </Box>
  );
};
export default DeleteJobs;
