import { useForm } from "react-hook-form";
import {
  JobSeekerUserDataType,
  ModifiedJobSeekerUserCreationDataType,
  UserCreateModeEnum,
} from "@/types/jobSeeker";
import { NoticePeriodEnum } from "contract/enum";
import { getQueryClient } from "api";
import CustomSkeleton from "@components/CustomSkeleton";
import { useEffect } from "react";
import isNil from "lodash/isNil";
import JobSeekerProfileForm, {
  defaultJobSeekerCreationData,
} from "@components/profile/jobSeeker/JobSeekerProfileForm";
import { useRouter } from "next/router";
import { useApi } from "@/hooks/useApi";
import { contract } from "contract";
import { useQueryClient } from "@tanstack/react-query";
import ErrorMessage from "@components/ErrorMessage";
import { getNoticePeriodLabel } from "@/utils/common";

export const noticePeriodOptions = Object.values(NoticePeriodEnum).map(
  (eachPeriod) => {
    return {
      label: getNoticePeriodLabel(eachPeriod),
      value: eachPeriod,
    };
  }
);
export const setNoticePeriodValue = (noticePeriod: string) => {
  const selectedOption = noticePeriodOptions.find(
    (option) => option.value === noticePeriod
  );
  return selectedOption;
};

const EditJobSeekerProfilePageComponent = () => {
  const { data, isLoading, error } =
    getQueryClient().user.getJobseekerProfile.useQuery(
      [contract.user.getJobseekerProfile.path],
      {}
    );
  if (isLoading) {
    return (
      <CustomSkeleton
        height={600}
        radius="xl"
        mx="auto"
        maw={{ base: 1037, xl: 1037 }}
        mt={{ base: 40, xl: 100 }}
      />
    );
  }
  if (error) {
    return <ErrorMessage error={error} />;
  }
  return <EditProfileComponent data={data.body} />;
};

const EditProfileComponent = ({ data }: { data: JobSeekerUserDataType }) => {
  const router = useRouter();
  const { makeApiCall } = useApi();
  const mutateQueryClient = useQueryClient();
  const hForm = useForm<ModifiedJobSeekerUserCreationDataType>({
    mode: "onChange",
    defaultValues: defaultJobSeekerCreationData,
  });
  const { reset } = hForm;
  const updateProfileData = ({
    payload,
  }: {
    payload: ModifiedJobSeekerUserCreationDataType;
  }) => {
    makeApiCall({
      fetcherFn: async () => {
        const response = await getQueryClient().user.updateJobseeker.mutation({
          body: {
            ...payload,
            city: !isNil(payload.city?.value) ? Number(payload.city?.value) : 0, // TODO : Need to replace sending default value 0
            expectedSalaryInLpa: payload.expectedSalaryInLpa
              ? Number(payload.expectedSalaryInLpa)
              : null,
            experienceInYear: !isNil(payload.experienceInYear)
              ? Number(payload.experienceInYear)
              : 0, // TODO : Need to replace sending default value 0
            noticePeriod: payload.noticePeriod?.value
              ? (payload.noticePeriod?.value as NoticePeriodEnum)
              : NoticePeriodEnum.immediately,
            preferredLocations:
              payload.preferredLocations?.map((eachValue) =>
                Number(eachValue.value)
              ) ?? [],
            languages:
              payload.languages?.map((eachValue) => Number(eachValue.value)) ??
              [],
            subfunction:
              payload.subfunction?.map((eachValue) =>
                Number(eachValue.value)
              ) ?? [],
            skills:
              payload.skills?.map((eachValue) => Number(eachValue.value)) ?? [],
            workSchedule: payload.workSchedule ?? null,
          },
        });
        return response;
      },
      successMsgProps: { message: "Profile saved successfully" },
      onSuccessFn: () => {
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.user.getJobseekerProfile.path],
        });
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.user.getUserProfile.path],
        });
        router.push("/profile");
      },
      showFailureMsg: true,
    });
  };
  useEffect(() => {
    reset({
      ...data,
      isSubscribedToAlerts: data.isSubscribedToAlerts,
      city: { label: data.city?.name, value: String(data.city?.id) },
      noticePeriod: setNoticePeriodValue(data.noticePeriod),
      experienceInYear: !isNil(data.experienceInYear)
        ? data.experienceInYear
        : 0,
      preferredLocations: data.preferredLocations.map((eachLocation) => ({
        label: eachLocation.name,
        value: String(eachLocation.id),
      })),
      languages: data.languages.map((eachLocation) => ({
        label: eachLocation.name,
        value: String(eachLocation.id),
      })),
      subfunction: data.subfunction.map((eachLocation) => ({
        label: eachLocation.name,
        value: String(eachLocation.id),
      })),
      skills: data.skills.map((eachLocation) => ({
        label: eachLocation.name,
        value: String(eachLocation.id),
      })),
      socialMediaLink: data.socialMediaLink,
      workSchedule: data.workSchedule ?? undefined,
    });
  }, [data]);
  return (
    <JobSeekerProfileForm
      hForm={hForm}
      onSubmit={updateProfileData}
      mode={UserCreateModeEnum.edit}
      onCancel={() => {
        router.push("/profile");
      }}
    />
  );
};

export default EditJobSeekerProfilePageComponent;
