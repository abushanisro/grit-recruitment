import {
  Avatar,
  Box,
  Grid,
  Group,
  Stack,
  Image,
  Divider,
  FileButton,
  CloseButton,
  Flex,
  Tooltip,
  Input,
} from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { UseFormReturn } from "react-hook-form";
import CustomInputField from "@components/form/CustomInputField";
import CustomTextArea from "@components/form/CustomTextArea";
import SecondaryButton from "@components/buttons/SecondaryButton";
import { notifications } from "@mantine/notifications";
import { useState } from "react";
import { fetchDataWithThrowError, useApi } from "@/hooks/useApi";
import { getApiUrl } from "@/env";
import { UserCreateModeEnum } from "@/types/jobSeeker";
import LabelText from "@/components/profile/LabelText";
import CustomNumberInputField from "@components/form/CustomNumberInput";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { IconExternalLink } from "@tabler/icons-react";
import AsyncSearchSelectField from "@components/form/AsyncSearchSelectField";
import { getQueryClient } from "api";
import { ModifiedEmployerUserCreationDataType } from "@/types/employer";
import { CompanySizeEnum, UserRole } from "contract/enum";
import ProfileFormContainer from "@/components/profile/ProfileFormContainer";
import VerificationDocumentFileUploadField from "@/components/profile/employer/VerificationDocumentFileUploadField";
import { checkIsValidUrl } from "@/utils/form";
import { ActiveTabButton } from "@components/buttons/ActiveTabButton";
import { getSelectFieldStyles } from "@/utils/profile";
import { UploadSuccessType } from "@/types/profile";
import getFilteredLocationOptionsData from "@/data/common";

const editProfileToastId = "editProfileToastId";

export const defaultEmployerData: ModifiedEmployerUserCreationDataType = {
  email: "",
  firstName: "",
  lastName: "",
  picture: null,
  phone: "",
  city: null,
  role: UserRole.employer,
  company: {
    name: "",
    website: null,
    industries: null,
    logo: null,
  },
  aboutCompany: "",
  companySize: null,
  verificationDocument: "",
  gstNumber: "",
  gstAddress: "",
};

export const companySizeOptions = [
  { label: "Self Employed", value: CompanySizeEnum.Selfemployed },
  { label: "1 to 30", value: CompanySizeEnum.OneToThirty },
  { label: "30 to 50", value: CompanySizeEnum.ThirtyToFifty },
  { label: "50 to 200", value: CompanySizeEnum.FiftyToTwoHundred },
  {
    label: "200 to 500",
    value: CompanySizeEnum.TwoHundredToFiveHundred,
  },
  {
    label: "500 to 600",
    value: CompanySizeEnum.FiveHundredToSixHundred,
  },
  { label: "600 to 800", value: CompanySizeEnum.SixHundredToEightHundred },
  { label: "800 to 1000", value: CompanySizeEnum.EightHundredToThousand },
  {
    label: "1000 to 3000",
    value: CompanySizeEnum.ThousandToThreeThousand,
  },
  {
    label: "3000 to 5000",
    value: CompanySizeEnum.ThreeThousandToFiveThousand,
  },
  {
    label: "5000 to 7000",
    value: CompanySizeEnum.FiveThousandToSevenThousand,
  },
  {
    label: "7000 to 10000",
    value: CompanySizeEnum.SevenThousandToTenThousand,
  },
  {
    label: "More than 10,000",
    value: CompanySizeEnum.MorethanTenTHousand,
  },
];
export const getCompanySizeLabel = (value: CompanySizeEnum) => {
  const option = companySizeOptions.find((option) => option.value === value);
  return option ? option.label : "";
};

const EmployerProfileForm = ({
  hForm,
  onSubmit,
  mode,
  onCancel,
}: {
  hForm: UseFormReturn<ModifiedEmployerUserCreationDataType>;
  onSubmit: (data: { payload: ModifiedEmployerUserCreationDataType }) => void;
  mode: UserCreateModeEnum;
  onCancel: () => void;
}) => {
  const { showToast } = useCustomToast();
  const { fetcherMakeApiCall } = useApi();
  const isTablet = useMediaQuery("(max-width: 992px)");
  const [companyLogoLoading, setCompanyLogoLoading] = useState<boolean>(false);
  const { setValue, watch, register, trigger } = hForm;

  const verificationDocument = watch("verificationDocument");
  const companyLogo = watch("company.logo");
  const handleSubmit = (values: ModifiedEmployerUserCreationDataType) => {
    if (!verificationDocument) {
      register("verificationDocument", {
        required: "Verification Document is required",
      }); // TODO : Need to replace with proper registering of field
      trigger("verificationDocument");
      return;
    }
    onSubmit({ payload: values });
  };

  const uploadFile = ({
    file,
    setValue,
    setLoading,
  }: {
    file: File;
    setValue: (uploadFileUrl: string) => void;
    setLoading: (loading: boolean) => void;
  }) => {
    setLoading(true);
    const formData = new FormData();
    formData.append("files", file);

    fetcherMakeApiCall({
      fetcherFn: async () => {
        const response: { data: UploadSuccessType[] } =
          await fetchDataWithThrowError(`${getApiUrl()}/upload/media`, {
            method: "POST",
            body: formData,
          });
        if (response) {
          setValue(response.data[0].url);
          notifications.clean();
          showToast({
            status: ToastStatus.success,
            id: editProfileToastId,
            message: "File attached successfully",
          });
          setLoading(false);
        }
      },
      finallyFn: () => {
        setLoading(false);
      },
    });
  };
  const validateFile = ({
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
    maxAllowedFileSize: { size: number; errorToastMessage: string };
    minAllowedFileSize: { size: number; errorToastMessage: string };
    acceptedFormats?: string[];
  }) => {
    if (file === null) {
      showToast({
        status: ToastStatus.error,
        id: editProfileToastId,
        message: "No file found",
      });
      return;
    }
    const fileNameParts = file.name.split(".");
    const fileExtension = fileNameParts[fileNameParts.length - 1];
    if (acceptedFormats && !acceptedFormats.includes(`.${fileExtension}`)) {
      showToast({
        status: ToastStatus.error,
        id: editProfileToastId,
        message: `Expected file of format ${acceptedFormats.join(
          ", "
        )} but recieved .${fileExtension}`,
      });
    }

    if (file.size > maxAllowedFileSize.size) {
      showToast({
        status: ToastStatus.error,
        id: editProfileToastId,
        message: maxAllowedFileSize.errorToastMessage,
      });
      return;
    } else if (file.size < minAllowedFileSize.size) {
      showToast({
        status: ToastStatus.error,
        id: editProfileToastId,
        message: minAllowedFileSize.errorToastMessage,
      });
      return;
    }

    var fr = new FileReader();
    const dateTimeNow = new Date().getTime();

    const myNewFile = new File(
      [file],
      `${fileNameParts[0]}${dateTimeNow}.${fileExtension}`,
      {
        type: file.type,
      }
    );
    fr.onload = function () {
      uploadFile({ file: myNewFile, setValue, setLoading });
      return;
    };

    fr.readAsDataURL(myNewFile);
  };
  return (
    <ProfileFormContainer
      sideLabelComponent={
        <ActiveTabButton
          isActive={true}
          label={
            mode === UserCreateModeEnum.create
              ? "My profile - Create Mode"
              : "My profile - Edit Mode"
          }
        />
      }
      formComponent={
        <form onSubmit={hForm.handleSubmit(handleSubmit)}>
          <Avatar
            pos={{ base: "relative", sm: "absolute" }}
            w={{ base: 80, md: 120, xl: 160 }}
            h={{ base: 80, md: 120, xl: 160 }}
            left={{
              base: "30px",
              sm: "60px",
            }}
            top={{
              base: "-40px",
              sm: "-50px",
              md: "-70px",
              lg: "-85px",
              xl: "-85px",
            }}
            bg="primaryGrey.1"
            style={{
              border: "1px solid",
              borderColor: "var(--mantine-color-secondaryGreen-1)",
            }}
            src={companyLogo ?? ""}
            alt="profile pic"
          />

          <Stack gap={8} pt={{ base: 30, xl: 100 }} pb={{ base: 30, xl: 60 }}>
            <FileButton
              onChange={(e) =>
                validateFile({
                  file: e,
                  setValue: (fileUrl: string) => {
                    setValue("company.logo", fileUrl);
                  },
                  setLoading: (loading: boolean) => {
                    setCompanyLogoLoading(loading);
                  },
                  maxAllowedFileSize: {
                    size: 5 * 1024 * 1024,
                    errorToastMessage: "Image size cannot be greater than 5MB",
                  },
                  minAllowedFileSize: {
                    size: 5 * 1024,
                    errorToastMessage: "Image size cannot be lesser than 5kB",
                  },
                })
              }
              accept="image/*"
            >
              {(props) => (
                <PrimaryButton
                  loading={companyLogoLoading}
                  loaderProps={{ type: "dots" }}
                  px={40}
                  label={
                    companyLogo ? "Add company logo" : "Upload company logo"
                  }
                  bg="primaryGrey.1"
                  tt="uppercase"
                  c="secondaryGreen.1"
                  style={{ borderRadius: 20 }}
                  maw={300}
                  {...props}
                />
              )}
            </FileButton>
            <Input.Description>
              {`Image of file size: 5kB - 5MB only.`}
            </Input.Description>
          </Stack>

          <Stack gap={isTablet ? 24 : 32}>
            <CustomInputField
              hForm={hForm}
              name="company.name"
              label="Company name"
              rules={{
                required: "Company name is required",
                validate: {
                  checkNameLength: (value) => {
                    if (value.length < 3 || value.length > 200) {
                      return "Company name must be between 3 and 200 characters.";
                    }
                    return true; // Validation passed
                  },
                },
              }}
            />
            <Grid gutter={isTablet ? 24 : 32}>
              <Grid.Col span={{ base: 12, md: 6 }}>
                <CustomInputField
                  hForm={hForm}
                  name="firstName"
                  placeholder="First Name"
                  label="First Name"
                  rules={{
                    required: "Name is required",
                    validate: {
                      checkNameLength: (value) => {
                        if (value.length < 3 || value.length > 50) {
                          return "First Name must be between 3 and 50 characters.";
                        }
                        return true; // Validation passed
                      },
                    },
                  }}
                />
              </Grid.Col>
              <Grid.Col span={{ base: 12, md: 6 }}>
                <CustomInputField
                  hForm={hForm}
                  name="lastName"
                  placeholder="Last Name"
                  label="Last Name"
                  rules={{
                    required: "Last Name is required",
                    validate: {
                      checkNameLength: (value) => {
                        if (value.length < 1 || value.length > 50) {
                          return "Last Name must be between 1 and 50 characters.";
                        }
                        return true; // Validation passed
                      },
                    },
                  }}
                />
              </Grid.Col>
            </Grid>
            <Grid gutter={isTablet ? 24 : 32}>
              <Grid.Col span={{ base: 12, md: 6 }}>
                <CustomInputField
                  hForm={hForm}
                  name="email"
                  label="Email"
                  rules={{ required: "Email is Required" }}
                  disabled
                />
              </Grid.Col>
              <Grid.Col span={{ base: 12, md: 6 }}>
                <CustomNumberInputField
                  hForm={hForm}
                  name="phone"
                  placeholder="Phone Number"
                  label="Phone Number"
                  type="tel"
                  hideControls
                  allowNegative={false}
                  allowDecimal={false}
                  rules={{
                    required: "Phone number is required",
                    validate: {
                      validatePhoneNumber: (value) => {
                        if (!/^[0-9]{10}$/.test(value)) {
                          return "Phone number must be exactly 10 digits.";
                        }
                        return true; // Validation passed
                      },
                    },
                  }}
                />
              </Grid.Col>
            </Grid>
            <Flex wrap="wrap" gap={10}>
              <Box miw={{ base: "100%", sm: 260 }}>
                <AsyncSearchSelectField
                  rules={{ required: "Location is Required" }}
                  hForm={hForm}
                  name="city"
                  label="Location"
                  placeholder="Location"
                  labelProps={{
                    color: "var(--mantine-color-secondaryGreen-1)",
                    fw: 700,
                    size: "md",
                    pb: 10,
                  }}
                  getOptions={async (val: string) => {
                    const data =
                      await getQueryClient().job.getSuggestionLocation.query({
                        query: { searchText: val },
                      });
                    if (data.status === 200) {
                      return getFilteredLocationOptionsData({
                        data: data.body,
                      });
                    }
                    return [];
                  }}
                  instanceId="location"
                  customStyles={{
                    control: () => ({
                      fontSize: isTablet ? 14 : 16,
                      backgroundColor: "var(--mantine-color-primaryGreen-3)",
                    }),
                  }}
                  dropDownIcon={
                    <Image
                      src="/images/downArrow.svg"
                      w={10}
                      h={8}
                      alt="indicator-icon"
                    />
                  }
                />
              </Box>
              <Box miw={{ base: "100%", sm: 300 }}>
                <AsyncSearchSelectField
                  rules={{ required: "Industry is Required" }}
                  hForm={hForm}
                  label="Industry"
                  labelProps={{
                    color: "var(--mantine-color-secondaryGreen-1)",
                    fw: 700,
                    size: "md",
                    pb: 10,
                  }}
                  name="company.industries"
                  placeholder="Industry"
                  isMulti
                  getOptions={async (val: string) => {
                    const data =
                      await getQueryClient().job.getSuggestionIndustry.query({
                        query: { searchText: val },
                      });
                    if (data.status === 200) {
                      return data.body.map((eachValue) => ({
                        label: eachValue.name,
                        value: eachValue.id.toString(),
                      }));
                    }
                    return [];
                  }}
                  instanceId="industry"
                  customStyles={{
                    control: () => ({
                      fontSize: isTablet ? 14 : 16,
                      backgroundColor: "var(--mantine-color-secondaryGreen-1)",
                    }),
                  }}
                  dropDownIcon={
                    <Image
                      src="/images/downArrow.svg"
                      w={10}
                      h={8}
                      alt="indicator-icon"
                    />
                  }
                />
              </Box>
            </Flex>

            <Flex justify="flex-start" wrap="wrap" gap={isTablet ? 30 : 60}>
              <Group>
                <LabelText label="Company Size" pb={0} />
                <Divider w={2} orientation="vertical" bg="secondaryGreen.1" />
                <Box miw={{ base: "100%", sm: 260 }}>
                  <AsyncSearchSelectField
                    hForm={hForm}
                    name="companySize"
                    placeholder="Select number of employees"
                    getOptions={async (val: string) => {
                      return companySizeOptions.filter((option) =>
                        option.label.toLowerCase().includes(val.toLowerCase())
                      );
                    }}
                    instanceId="companySize"
                    customStyles={getSelectFieldStyles(isTablet)}
                    dropDownIcon={
                      <Image
                        src="/images/greenDownArrow.svg"
                        w={10}
                        h={8}
                        alt="indicator-icon"
                      />
                    }
                  />
                </Box>
              </Group>

              <Group>
                <LabelText label="Website" pb={0} />
                <Divider w={2} orientation="vertical" bg="secondaryGreen.1" />
                <Box miw={160} maw={320}>
                  <CustomInputField
                    hForm={hForm}
                    name="company.website"
                    placeholder="Add link to website"
                    rules={{
                      validate: {
                        isValidUrl: (value) => {
                          return checkIsValidUrl(value);
                        },
                      },
                    }}
                    inputStyles={{
                      styles: {
                        input: {
                          color: "white",
                          background: "transparent",
                          borderRadius: 30,
                          fontSize: isTablet ? 16 : 20,
                          paddingInline: 20,
                          paddingBlock: 20,
                        },
                        section: { marginLeft: 10, zIndex: 0 },
                      },
                    }}
                  />
                </Box>
              </Group>
            </Flex>
            <CustomTextArea
              hForm={hForm}
              name="aboutCompany"
              label="About Company"
              inputStyles={{
                styles: {
                  input: {
                    color: "white",
                    background: "transparent",
                    borderRadius: 20,
                    fontSize: isTablet ? 16 : 20,
                    paddingInline: 20,
                    paddingBlock: 20,
                  },
                },
              }}
              placeholder="Highlight your mission, values, and distinctive qualities to attract top talent"
              autosize
              minRows={4}
              maxRows={4}
              rules={{
                required: "About Company is required",
                validate: {
                  checkNameLength: (value) => {
                    if (value.length < 10 || value.length > 2000) {
                      return "About company section must be between 10 and 2000 characters.";
                    }
                    return true; // Validation passed
                  },
                },
              }}
            />
            <Box
              bg="#1f3243"
              px={40}
              py={60}
              style={{ borderRadius: 15 }}
              maw={{ base: "100%" }}
            >
              <Stack gap={16}>
                <LabelText fz={19} label="GST Information" />
                <CustomInputField
                  hForm={hForm}
                  name="gstNumber"
                  placeholder="Please enter your company GST number"
                  label="GST Number"
                  rules={{
                    required: "GST Number is required",
                    validate: {
                      checkNameLength: (value) => {
                        if (
                          !/^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}[Z]{1}[0-9A-Z]{1}$/.test(
                            value
                          )
                        ) {
                          return "Please enter a valid GST number";
                        }
                        return true; // Validation passed
                      },
                    },
                  }}
                />
                <CustomTextArea
                  hForm={hForm}
                  name="gstAddress"
                  label="Address"
                  inputStyles={{
                    styles: {
                      input: {
                        color: "white",
                        background: "transparent",
                        borderRadius: 20,
                        fontSize: isTablet ? 16 : 20,
                        paddingInline: 20,
                        paddingBlock: 20,
                      },
                    },
                  }}
                  placeholder="Please enter your address"
                  autosize
                  minRows={4}
                  maxRows={4}
                  rules={{
                    required: "Address is required",
                    validate: {
                      checkNameLength: (value) => {
                        if (value.length < 20 || value.length > 500) {
                          return "Address must be between 20 and 500 characters.";
                        }
                        return true; // Validation passed
                      },
                    },
                  }}
                />
              </Stack>
            </Box>

            {!verificationDocument ? (
              <VerificationDocumentFileUploadField
                hForm={hForm}
                validateFile={validateFile}
              />
            ) : (
              <Box>
                <LabelText
                  rules={{
                    required: "Upload Verification Document is required",
                  }}
                  label={"Upload Verification Document"}
                />
                <Group align="center" w={{ base: "100%", sm: 370 }}>
                  <Tooltip label={`${verificationDocument.slice(0, 20)}...`}>
                    <a
                      href={verificationDocument}
                      target="_blank"
                      style={{
                        cursor: "pointer",
                        textDecoration: "none",
                        width: isTablet ? "100%" : "max-content",
                      }}
                    >
                      <PrimaryButton
                        fz={{ base: 14, sm: 16 }}
                        px={10}
                        w="100%"
                        label={"Uploaded Verification Document"}
                        bg="primaryGrey.1"
                        c="secondaryGreen.1"
                        style={{ borderRadius: 20 }}
                        leftSection={
                          <IconExternalLink size={isTablet ? 16 : 30} />
                        }
                        rightSection={
                          <CloseButton
                            c="secondaryGreen.1"
                            bg="none"
                            onClick={(e) => {
                              e.preventDefault();
                              setValue("verificationDocument", "");
                            }}
                          />
                        }
                      />
                    </a>
                  </Tooltip>
                </Group>
              </Box>
            )}
            <Box>
              <PrimaryButton
                label={mode === UserCreateModeEnum.create ? "Create" : "Update"}
                fz={{ base: 16, sm: 18 }}
                fw="600"
                px={20}
                py={12}
                w={{ base: "100%", sm: "max-content" }}
                h="max-content"
                type="submit"
              />

              <SecondaryButton
                label="Cancel"
                fz={{ base: 16, sm: 18 }}
                fw="600"
                px={20}
                py={12}
                mt={{ base: 10, sm: 0 }}
                ml={{ base: 0, sm: 20 }}
                w={{ base: "100%", sm: "max-content" }}
                h="max-content"
                onClick={onCancel}
              />
            </Box>
          </Stack>
        </form>
      }
    />
  );
};

export default EmployerProfileForm;
