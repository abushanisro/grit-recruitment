import { ModifiedEmployerUserCreationDataType } from "@/types/employer";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { FileButton, Input, Stack, VisuallyHidden } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { IconUpload } from "@tabler/icons-react";
import { useState } from "react";
import { UseFormReturn } from "react-hook-form";

const VerificationDocumentFileUploadField = ({
  hForm,
  validateFile,
}: {
  hForm: UseFormReturn<ModifiedEmployerUserCreationDataType>;
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
  const [
    verificationDocumentUploadLoading,
    setVerificationDocumentUploadLoading,
  ] = useState<boolean>(false);
  const {
    setValue,
    register,
    formState: { errors },
  } = hForm;

  return (
    <Stack>
      <FileButton
        onChange={(e) =>
          validateFile({
            file: e,
            setValue: (fileUrl: string) => {
              setValue("verificationDocument", fileUrl);
            },
            setLoading: (loading: boolean) =>
              setVerificationDocumentUploadLoading(loading),
            maxAllowedFileSize: {
              size: 5 * 1024 * 1024,
              errorToastMessage: "Document size cannot be greater than 5MB",
            },
            minAllowedFileSize: {
              size: 5 * 1024,
              errorToastMessage: "Document size cannot be lesser than 5kB",
            },
            acceptedFormats: [".pdf", ".doc", ".docx"],
          })
        }
        accept=".pdf,.doc,.docx"
      >
        {(props) => (
          <PrimaryButton
            loading={verificationDocumentUploadLoading}
            loaderProps={{
              type: "dots",
              size: 30,
              style: {
                background: "var(--mantine-color-primaryGrey-1)",
                color: "var(--mantine-color-secondaryGreen-1)",
                minWidth: tablet ? 220 : 300,
                height: 40,
                borderRadius: 10,
              },
            }}
            w={{ base: "100%", sm: "max-content" }}
            fw={400}
            fz={16}
            label="Upload Verification Document *"
            px={20}
            leftSection={<IconUpload size={20} />}
            style={{ borderRadius: 20, textOverflow: "ellipsis" }}
            bg="primaryGrey.1"
            c="secondaryGreen.1"
            {...props}
          />
        )}
      </FileButton>{" "}
      <Input.Description maw={{ base: 200, lg: 460 }}>
        {`Accepted formats: .pdf, .doc, .docx. File size : 5kB - 5MB only.`}
      </Input.Description>
      {errors.verificationDocument && (
        <>
          <VisuallyHidden>
            <input {...register("verificationDocument")} />
          </VisuallyHidden>
          <Input.Error fz={{ base: 14, sm: 16 }}>
            {errors.verificationDocument.message ?? "Field is Required"}
          </Input.Error>
        </>
      )}
    </Stack>
  );
};
export default VerificationDocumentFileUploadField;
