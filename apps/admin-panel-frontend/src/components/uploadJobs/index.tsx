import { useApi } from "@/hooks/useApi";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { FileUploadFormInterface } from "@/types/fileUpload";
import { Anchor, Box, Group, List, Text } from "@mantine/core";
import { getQueryClient } from "api";
import { useState } from "react";
import { useForm } from "react-hook-form";
import JobDragAndDrop from "@/components/uploadJobs/JobDragAndDrop";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { useMediaQuery } from "@mantine/hooks";
import { getS3BucketBaseUrl } from "@/env";

const acceptedFileTypes = [".xlsx", ".xls"];
const defaultFormValues: FileUploadFormInterface = {
  file: null,
};
export const fileUploadToastId = "fileUploadToastId";
const maxAllowedSizeInMb = 5;

const UploadJobs = () => {
  const [isHovered, setIsHovered] = useState<boolean>(false);
  const [fileUploadErrors, setFileUploadErrors] = useState<string[]>([]);
  const hForm = useForm<FileUploadFormInterface>({
    mode: "onChange",
    defaultValues: defaultFormValues,
  });
  const isTablet = useMediaQuery("(max-width: 768px)");
  const { setValue, watch } = hForm;
  const selectedFile = watch("file");
  const { showToast } = useCustomToast();
  const { makeApiCall } = useApi();
  const handleValidateUploadedFile = (file: File | null) => {
    if (file === null) {
      showToast({
        status: ToastStatus.error,
        id: fileUploadToastId,
        message: "File can't be uploaded",
      });
      return false;
    }
    const arrayOfFile = file.name.split(".");
    const currentFileType = `.${arrayOfFile[arrayOfFile.length - 1]}`;
    if (
      !acceptedFileTypes.some(
        (eachFileType) => eachFileType === currentFileType
      )
    ) {
      showToast({
        status: ToastStatus.error,
        id: fileUploadToastId,
        message: `Invalid file type.Please upload file of type ${acceptedFileTypes.join(
          ","
        )}`,
      });
    }

    const maxAllowedSize = maxAllowedSizeInMb * 1024 * 1024;
    if (file.size > maxAllowedSize) {
      showToast({
        status: ToastStatus.error,
        id: fileUploadToastId,
        message: `File larger than ${maxAllowedSizeInMb}MB`,
      });

      return false;
    }
    return true;
  };

  const handleUploadFile = ({ file }: { file: File }) => {
    setFileUploadErrors([]);
    const dateTimeNow = new Date().getTime();
    const arrayOfFile = file.name.split(".");

    const myNewFile = new File(
      [file],
      `${arrayOfFile[0]}${dateTimeNow}.${arrayOfFile[arrayOfFile.length - 1]}`,
      {
        type: file.type,
      }
    );
    const formData = new FormData();
    formData.append("file", myNewFile);
    makeApiCall({
      fetcherFn: async () => {
        const response =
          await getQueryClient().job.bulkUploadJobsForJobSeeker.mutation({
            body: formData,
          });

        return response;
      },
      onSuccessFn: (response) => {
        if (response.status !== 200) {
          return;
        }
        if (response.body.kind === "success") {
          setValue("file", null);
          setFileUploadErrors([]);
          showToast({
            status: ToastStatus.success,
            id: fileUploadToastId,
            message: "Jobs uploaded successfully",
          });
        }
        if (response.body.kind === "error") {
          showToast({
            status: ToastStatus.error,
            id: fileUploadToastId,
            message: "Encountered errors while uploading jobs",
          });
          setFileUploadErrors(response.body.errors);
        }
      },
      showFailureMsg: true,
    });
  };
  return (
    <Box mt={80}>
      <Box>
        <Text fz={{ base: 20, sm: 24 }} fw={600} lh="1.16" mb={20} ta="center">
          Upload jobs for jobseekers
        </Text>
      </Box>

      <Box pt={60} mb={20}>
        <JobDragAndDrop
          hForm={hForm}
          name="file"
          acceptedFileTypes={acceptedFileTypes.join(",")}
          onFileSelect={(file) => {
            if (!handleValidateUploadedFile(file)) {
              return;
            }
            setValue("file", file);
          }}
          removeFile={() => {
            setValue("file", null);
          }}
        />
      </Box>
      <Group justify={isTablet ? "center" : "space-between"}>
        <Anchor
          onMouseEnter={() => setIsHovered(true)}
          onMouseLeave={() => setIsHovered(false)}
          href={`${getS3BucketBaseUrl()}/admin/JobseekerJobs.xlsx`}
          download
          style={{ textDecoration: "underline" }}
          fw="semibold"
          fz={16}
          lh="18px"
          c={isHovered ? "#ECBA46" : "secondaryYellow.3"}
        >
          Download the file format for jobseeer
        </Anchor>
        <PrimaryButton
          onClick={() => {
            if (!selectedFile) {
              showToast({
                id: fileUploadToastId,
                message: "Please select a file to upload",
                status: ToastStatus.error,
              });
              return;
            }
            handleUploadFile({
              file: selectedFile,
            });
          }}
          label="Upload Now"
        />
      </Group>

      {fileUploadErrors.length > 0 && (
        <List mt={10}>
          {fileUploadErrors?.map((eachError, index) => {
            return <List.Item key={index}>{eachError}</List.Item>;
          })}
        </List>
      )}
    </Box>
  );
};
export default UploadJobs;
