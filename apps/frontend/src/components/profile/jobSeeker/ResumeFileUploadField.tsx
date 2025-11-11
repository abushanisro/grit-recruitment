import { ModifiedJobSeekerUserCreationDataType } from "@/types/jobSeeker";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { FileButton, Input, Stack, VisuallyHidden } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { IconUpload } from "@tabler/icons-react";
import { useState } from "react";
import { UseFormReturn } from "react-hook-form";

const ResumeFileUploadField = ({
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
              setValue("resume", fileUrl);
            },
            setLoading: (loading: boolean) => setResumeUploadLoading(loading),
            maxAllowedFileSize: {
              size: 5 * 1024 * 1024,
              errorToastMessage: "Resume file size cannot be greater than 5MB",
            },
            minAllowedFileSize: {
              size: 5 * 1024,
              errorToastMessage: "Resume file size cannot be lesser than 5kB",
            },
            acceptedFormats: [".pdf", ".doc", ".docx"],
          })
        }
        accept=".pdf,.doc,.docx"
      >
        {(props) => (
          <PrimaryButton
            loading={resumeUploadLoading}
            loaderProps={{
              type: "dots",
              size: 30,
              style: {
                background: "var(--mantine-color-customBlack-1)",
                color: "white",
                width: 240,
                height: 40,
                borderRadius: 10,
              },
            }}
            w="max-content"
            fz={{ base: 16, sm: 20 }}
            label="Upload Resume *"
            px={20}
            leftSection={<IconUpload size={20} />}
            style={{ borderRadius: 20 }}
            {...props}
          />
        )}
      </FileButton>{" "}
      <Input.Description maw={{ base: 200, lg: 460 }}>
        {`Accepted formats: .pdf, .doc, .docx. File size : 5kB - 5MB only.`}
      </Input.Description>
      {errors.resume && (
        <>
          <VisuallyHidden>
            <input {...register("resume")} />
          </VisuallyHidden>
          <Input.Error fz={{ base: 14, sm: 16 }}>
            {errors.resume.message ?? "Field is Required"}
          </Input.Error>
        </>
      )}
    </Stack>
  );
};
export default ResumeFileUploadField;
