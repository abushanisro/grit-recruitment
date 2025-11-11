import { ModifiedJobSeekerUserCreationDataType } from "@/types/jobSeeker";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { FileButton, Input, Stack, VisuallyHidden } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { IconUpload } from "@tabler/icons-react";
import { useState } from "react";
import { UseFormReturn } from "react-hook-form";

const VideoResumeFileUploadField = ({
  hForm,
  validateFile,
}: {
  hForm: UseFormReturn<ModifiedJobSeekerUserCreationDataType>;
  validateFile: ({
    file,
    setValue,
    setLoading,
    maxAllowedFileSize,
    minAllowedFileSize,
    acceptedFormats,
  }: {
    file: File | null;
    setValue: (fileUrl: string) => void;
    setLoading: (loading: boolean) => void;
    maxAllowedFileSize: {
      size: number;
      errorToastMessage: string;
    };
    minAllowedFileSize: {
      size: number;
      errorToastMessage: string;
    };
    acceptedFormats?: string[] | undefined;
  }) => void;
}) => {
  const tablet = useMediaQuery("(max-width: 992px)");
  const [resumeUploadLoading, setResumeUploadLoading] =
    useState<boolean>(false);
  const {
    setValue,
    register,
    formState: { errors },
  } = hForm;
  return (
    <Stack align={tablet ? "flex-start" : "flex-end"}>
      <FileButton
        onChange={(e) =>
          validateFile({
            file: e,
            setValue: (fileUrl: string) => {
              setValue("videoResume", fileUrl);
            },
            setLoading: (loading: boolean) => setResumeUploadLoading(loading),
            maxAllowedFileSize: {
              size: 15 * 1024 * 1024,
              errorToastMessage:
                "videoResume file size cannot be greater than 15MB",
            },
            minAllowedFileSize: {
              size: 1 * 1024 * 1024,
              errorToastMessage:
                "videoResume file size cannot be lesser than 1mb",
            },
            acceptedFormats: [".mp4"],
          })
        }
        accept=".mp4"
      >
        {(props) => (
          <PrimaryButton
            loading={resumeUploadLoading}
            loaderProps={{
              type: "dots",
              size: 30,
              style: {
                background: "var(--mantine-color-primarySkyBlue-6)",
                color: "white",
                width: 240,
                height: 40,
                borderRadius: 10,
              },
            }}
            w="max-content"
            fz={{ base: 16, sm: 20 }}
            label="Upload Video Resume *"
            px={20}
            leftSection={<IconUpload size={20} />}
            style={{ borderRadius: 20 }}
            {...props}
          />
        )}
      </FileButton>{" "}
      <Input.Description maw={{ base: 200, lg: 460 }}>
        {`Accepted formats: .mp4 File size : 1 mb - 15 mb only.`}
      </Input.Description>
      {errors.videoResume && (
        <>
          <VisuallyHidden>
            <input {...register("videoResume")} />
          </VisuallyHidden>
          <Input.Error fz={{ base: 14, sm: 16 }}>
            {errors.videoResume.message ?? "Field is Required"}
          </Input.Error>
        </>
      )}
    </Stack>
  );
};
export default VideoResumeFileUploadField;
