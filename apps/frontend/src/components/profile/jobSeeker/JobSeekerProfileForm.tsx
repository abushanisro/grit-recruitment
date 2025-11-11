import {
  Avatar,
  Box,
  Grid,
  Group,
  Stack,
  Image,
  Text,
  Divider,
  FileButton,
  CloseButton,
  Flex,
  Tooltip,
  Input,
} from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { IconExternalLink } from "@tabler/icons-react";
import { UseFormReturn } from "react-hook-form";
import CustomInputField from "@components/form/CustomInputField";
import CustomTextArea from "@components/form/CustomTextArea";
import AsyncSearchSelectField from "@components/form/AsyncSearchSelectField";
import { getQueryClient } from "api";
import SecondaryButton from "@components/buttons/SecondaryButton";
import { notifications } from "@mantine/notifications";
import { useState } from "react";
import { fetchDataWithThrowError, useApi } from "@/hooks/useApi";
import { getApiUrl } from "@/env";
import {
  ModifiedJobSeekerUserCreationDataType,
  UserCreateModeEnum,
} from "@/types/jobSeeker";
import LabelText from "@/components/profile/LabelText";
import AsyncSelectFieldWithOptionsOutside from "@/components/profile/AsyncSelectFieldWithOptionsOutside";
import CustomNumberInputField from "@components/form/CustomNumberInput";
import CustomCheckbox from "@components/form/CustomCheckbox";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { getSelectFieldStyles } from "@/utils/profile";
import { noticePeriodOptions } from "@/components/profile/jobSeeker/EditJobSeekerProfilePageComponent";
import ProfileFormContainer from "@/components/profile/ProfileFormContainer";
import ResumeFileUploadField from "@/components/profile/jobSeeker/ResumeFileUploadField";
import { checkIsValidUrl } from "@/utils/form";
import { ActiveTabButton } from "@components/buttons/ActiveTabButton";
import InlineLabelWithInputField from "@/components/profile/InlineLabelWithInputField";
import isEmpty from "lodash/isEmpty";
import { UploadSuccessType } from "@/types/profile";
import getFilteredLocationOptionsData, {
  getLocationOptionsData,
} from "@/data/common";
import VideoResumeFileUploadField from "@/components/profile/jobSeeker/VideoResumeFileUploadField";
import classes from "@/components/form/Form.module.css";
const editProfileToastId = "editProfileToastId";

export const defaultJobSeekerCreationData: ModifiedJobSeekerUserCreationDataType =
  {
    firstName: "",
    lastName: "",
    email: "",
    phone: "",
    resume: "",
    videoResume: "",
    picture: null,
    profileSummary: "",
    headline: "",
    expectedSalaryInLpa: null,
    isPrivate: false,
    externalLink: null,
    socialMediaLink: null,
    preferredLocations: null,
    experienceInYear: null,
    languages: null,
    subfunction: null,
    city: null,
    skills: null,
    noticePeriod: null,
    workSchedule: null,
    isSubscribedToAlerts: false,
  };

const JobSeekerProfileForm = ({
  hForm,
  onSubmit,
  mode,
  onCancel,
}: {
  hForm: UseFormReturn<ModifiedJobSeekerUserCreationDataType>;
  onSubmit: (data: { payload: ModifiedJobSeekerUserCreationDataType }) => void;
  mode: UserCreateModeEnum;
  onCancel: () => void;
}) => {
  const { showToast } = useCustomToast();
  const { fetcherMakeApiCall } = useApi();
  const isTablet = useMediaQuery("(max-width: 992px)");
  const [profilePicLoading, setProfilePicLoading] = useState<boolean>(false);
  const { setValue, watch, register, trigger } = hForm;

  const resumeUrl = watch("resume");
  const videoResumeUrl = watch("videoResume");
  const picture = watch("picture");
  const handleSubmit = (values: ModifiedJobSeekerUserCreationDataType) => {
    if (!resumeUrl) {
      register("resume", { required: "Resume is required" }); // TODO : Need to replace with proper registering of field
      trigger("resume");
      window.scrollTo({
        top: 0,
        behavior: "smooth", // Smooth scrolling animation
      });
      return;
    } else if (!videoResumeUrl) {
      register("videoResume", { required: "video Resume is required" }); // TODO : Need to replace with proper registering of field
      trigger("videoResume");
      window.scrollTo({
        top: 0,
        behavior: "smooth", // Smooth scrolling animation
      });
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
            bg="customGray.2"
            style={{
              border: "1px solid",
              borderColor: "var(--mantine-color-secondaryGreen-1)",
            }}
            src={picture ?? ""}
            alt="profile picture"
          />

          <Group
            justify="space-between"
            align="flex-start"
            pt={{ base: 30, xl: 100 }}
            pb={{ base: 30, xl: 60 }}
          >
            <Stack gap={8}>
              <FileButton
                onChange={(e) =>
                  validateFile({
                    file: e,
                    setValue: (fileUrl: string) => {
                      setValue("picture", fileUrl);
                    },
                    setLoading: (loading: boolean) => {
                      setProfilePicLoading(loading);
                    },
                    maxAllowedFileSize: {
                      size: 5 * 1024 * 1024,
                      errorToastMessage:
                        "Image size cannot be greater than 5MB",
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
                    loading={profilePicLoading}
                    loaderProps={{ type: "dots" }}
                    px={20}
                    label={picture ? "Update photo" : "Add Photo"}
                    tt="uppercase"
                    bg="primaryGrey.1"
                    c="secondaryGreen.1"
                    style={{ borderRadius: 20 }}
                    {...props}
                  />
                )}
              </FileButton>
              <Input.Description>
                {`Image of file size: 5kB - 5MB only.`}
              </Input.Description>
            </Stack>
            <Stack>
              {!resumeUrl ? (
                <ResumeFileUploadField
                  hForm={hForm}
                  validateFile={validateFile}
                />
              ) : (
                <Group
                  align="center"
                  bg="primarySkyBlue.6"
                  style={{ borderRadius: 30 }}
                  px={{ base: 10, sm: 20 }}
                  py={0}
                  w={{ base: 240, sm: 330 }}
                >
                  <Tooltip label={`${resumeUrl.slice(0, 20)}...`}>
                    <a
                      href={resumeUrl}
                      target="_blank"
                      style={{
                        cursor: "pointer",
                        textDecoration: "none",
                      }}
                    >
                      <PrimaryButton
                        fz={{ base: 14, sm: 16 }}
                        px={10}
                        label={
                          isTablet ? "Resume link" : "Uploaded Resume link"
                        }
                        bg="primarySkyBlue.6"
                        c="secondaryDarkBlue.9"
                        style={{ borderRadius: 20 }}
                        leftSection={
                          <IconExternalLink size={isTablet ? 16 : 30} />
                        }
                        rightSection={
                          <CloseButton
                            variant="transparent"
                            onClick={(e) => {
                              e.preventDefault();
                              setValue("resume", "");
                            }}
                          />
                        }
                      />
                    </a>
                  </Tooltip>
                </Group>
              )}

              {!videoResumeUrl ? (
                <VideoResumeFileUploadField
                  hForm={hForm}
                  validateFile={validateFile}
                />
              ) : (
                <Group
                  align="center"
                  bg="primarySkyBlue.6"
                  style={{ borderRadius: 30 }}
                  px={{ base: 10, sm: 20 }}
                  py={0}
                  w={{ base: 240, sm: 330 }}
                >
                  <Tooltip label={`${videoResumeUrl.slice(0, 20)}...`}>
                    <a
                      href={videoResumeUrl}
                      target="_blank"
                      style={{
                        cursor: "pointer",
                        textDecoration: "none",
                      }}
                    >
                      <PrimaryButton
                        fz={{ base: 14, sm: 16 }}
                        ml={{ base: 0, sm: -20 }}
                        px={10}
                        label={
                          isTablet
                            ? "Video Resume link"
                            : "Uploaded Video Resume link"
                        }
                        bg="primarySkyBlue.6"
                        c="secondaryDarkBlue.9"
                        style={{ borderRadius: 20 }}
                        leftSection={
                          <IconExternalLink size={isTablet ? 16 : 30} />
                        }
                        rightSection={
                          <CloseButton
                            variant="transparent"
                            onClick={(e) => {
                              e.preventDefault();
                              setValue("videoResume", "");
                            }}
                          />
                        }
                      />
                    </a>
                  </Tooltip>
                </Group>
              )}
            </Stack>
          </Group>
          <Stack gap={isTablet ? 24 : 32}>
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
            <CustomInputField
              hForm={hForm}
              name="headline"
              label="Profile Headline"
              rules={{
                required: "Profile headline is required",
                validate: {
                  checkNameLength: (value) => {
                    if (value.length < 10 || value.length > 150) {
                      return "Profile headline must be between 10 and 150 characters.";
                    }
                    return true; // Validation passed
                  },
                },
              }}
            />
            <Grid gutter={isTablet ? 24 : 32}>
              <Grid.Col span={{ base: 12, md: 8 }}>
                <CustomInputField
                  hForm={hForm}
                  name="email"
                  label="Email"
                  rules={{ required: "Email is Required" }}
                  disabled
                />
              </Grid.Col>
              <Grid.Col span={{ base: 12, md: 4 }}>
                <CustomNumberInputField
                  hForm={hForm}
                  className={classes.numberInput}
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
            <InlineLabelWithInputField
              label="Expected Salary"
              isRequired={true}
            >
              <CustomNumberInputField
                hForm={hForm}
                className={classes.numberInput}
                allowNegative={false}
                allowDecimal={false}
                name="expectedSalaryInLpa"
                placeholder="E.g 1"
                maw={130}
                miw={100}
                rules={{
                  required: true,
                  validate: {
                    checkBoundary: (value) => {
                      if (isEmpty(value)) {
                        return true;
                      }
                      if (value < 1 || value > 90) {
                        return "Expected Salary must be greater than 0 and lesser than 90";
                      }
                      return true; // Validation passed
                    },
                  },
                }}
                rightSection={
                  <Text fz={{ base: 17 }} fw={700}>
                    LPA
                  </Text>
                }
                inputStyles={{ styles: { section: { margin: 20 } } }}
              />
            </InlineLabelWithInputField>
            <CustomTextArea
              hForm={hForm}
              name="profileSummary"
              label="About You"
              placeholder="Write a little something about yourself, your experience and your achievements!"
              minRows={4}
              maxRows={4}
              rules={{
                validate: {
                  checkNameLength: (value) => {
                    if (!value) {
                      return true;
                    }
                    if (value.length < 10 || value.length > 2000) {
                      return "About you section must be between 10 and 2000 characters.";
                    }
                    return true; // Validation passed
                  },
                },
              }}
            />
            <AsyncSelectFieldWithOptionsOutside
              hForm={hForm}
              name="skills"
              label="Skills"
              getOptions={async (val: string) => {
                const data =
                  await getQueryClient().job.getSuggestionSkill.query({
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
            />

            <Flex justify="flex-start" wrap="wrap" gap={isTablet ? 40 : 60}>
              <Group>
                <LabelText
                  label="Years of Experience"
                  mt={10}
                  rules={{ required: "Years of Experience is required" }}
                />
                <Divider w={2} orientation="vertical" bg="secondaryGreen.1" />
                <CustomNumberInputField
                  hForm={hForm}
                  className={classes.numberInput}
                  allowNegative={false}
                  allowDecimal={false}
                  name="experienceInYear"
                  placeholder="E.g 1"
                  maw={130}
                  miw={100}
                  min={0}
                  max={50}
                  rules={{
                    required: "Years of Experience is required",
                    validate: {
                      checkBoundary: (value) => {
                        const numValue = parseFloat(value);
                        if (isNaN(numValue)) {
                          return "Please enter a valid number.";
                        }
                        if (numValue < 0 || numValue > 50) {
                          return "Years of Experience must be between 0 and 50.";
                        }
                        return true; // Validation passed
                      },
                    },
                  }}
                  rightSection={
                    <Text fz={{ base: 17 }} fw={700}>
                      years
                    </Text>
                  }
                  inputStyles={{ styles: { section: { margin: 20 } } }}
                />
              </Group>

              <Group>
                <LabelText
                  label="Notice Period"
                  mt={10}
                  rules={{ required: "Notice period is required" }}
                />
                <Divider w={2} orientation="vertical" bg="secondaryGreen.1" />
                <Box miw={160} maw={180}>
                  <AsyncSearchSelectField
                    hForm={hForm}
                    name="noticePeriod"
                    rules={{ required: "Notice period is required" }}
                    placeholder="Select"
                    getOptions={async (val: string) => {
                      return noticePeriodOptions.filter((option) =>
                        option.label.toLowerCase().includes(val.toLowerCase())
                      );
                    }}
                    instanceId="noticePeriod"
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
            </Flex>

            <Box maw={{ base: "100%", sm: "60%" }}>
              <LabelText
                label="Current Location"
                rules={{ required: "Current location is required" }}
              />
              <AsyncSearchSelectField
                hForm={hForm}
                name="city"
                placeholder="Current Location"
                rules={{ required: "Current location is required" }}
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
                instanceId="city"
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
            <AsyncSelectFieldWithOptionsOutside
              hForm={hForm}
              name="preferredLocations"
              label="Preferred Locations"
              placeholder="Type here"
              getOptions={async (val: string) => {
                const data =
                  await getQueryClient().job.getSuggestionLocation.query({
                    query: { searchText: val },
                  });
                if (data.status === 200) {
                  return getLocationOptionsData({
                    data: data.body,
                  });
                }
                return [];
              }}
            />
            <AsyncSelectFieldWithOptionsOutside
              hForm={hForm}
              name="languages"
              label="Languages"
              placeholder="Type here"
              rules={{ required: "Language is required" }}
              getOptions={async (val: string) => {
                const data =
                  await getQueryClient().job.getSuggestionLanguages.query({
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
            />
            <CustomInputField
              hForm={hForm}
              name="externalLink"
              placeholder="Add link to work samples/portfolio"
              rules={{
                validate: {
                  isValidUrl: (value) => {
                    return checkIsValidUrl(value);
                  },
                },
              }}
              leftSection={
                <Image
                  src="/images/link.svg"
                  alt="icon"
                  w={20}
                  h={20}
                  style={{ zIndex: 1 }}
                />
              }
              inputStyles={{
                styles: {
                  input: {
                    color: "white",
                    background: "transparent",
                    borderRadius: 30,
                    fontSize: isTablet ? 16 : 20,
                    paddingInline: 20,
                    paddingBlock: 20,
                    paddingLeft: 60,
                  },
                  section: { marginLeft: 10, zIndex: 0 },
                },
              }}
            />
            <CustomInputField
              hForm={hForm}
              name="socialMediaLink"
              placeholder="Add link to your social media"
              rules={{
                validate: {
                  isValidUrl: (value) => {
                    return checkIsValidUrl(value);
                  },
                },
              }}
              leftSection={
                <Image src="/images/socialLink.svg" alt="icon" w={20} h={20} />
              }
              inputStyles={{
                styles: {
                  input: {
                    color: "white",
                    background: "transparent",
                    borderRadius: 30,
                    fontSize: isTablet ? 16 : 20,
                    paddingInline: 20,
                    paddingBlock: 20,
                    paddingLeft: 60,
                  },
                  section: { marginLeft: 10, zIndex: 0 },
                },
              }}
            />

            <CustomCheckbox
              name="isSubscribedToAlerts"
              hForm={hForm}
              label="Do you want email job alerts based on the selected preferences"
            />
            <Box>
              <PrimaryButton
                label={mode === UserCreateModeEnum.create ? "Create" : "Update"}
                fz={{ base: 16, sm: 20 }}
                fw="600"
                px={20}
                py={12}
                w={{ base: "100%", sm: "max-content" }}
                h="max-content"
                type="submit"
              />
              <SecondaryButton
                styles={{
                  root: {
                    border: "1px solid",
                    borderColor: "var(--mantine-color-primarySkyBlue-6)",
                  },
                }}
                label="Cancel"
                fz={{ base: 16, sm: 20 }}
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

export default JobSeekerProfileForm;
