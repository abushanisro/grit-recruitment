import { GoogleUserDataType } from "@/types/auth";
import {
  ModifiedJobSeekerUserCreationDataType,
  UserCreateModeEnum,
} from "@/types/jobSeeker";
import JobSeekerProfileForm, {
  defaultJobSeekerCreationData,
} from "@components/profile/jobSeeker/JobSeekerProfileForm";
import { getQueryClient } from "api";
import isNil from "lodash/isNil";
import { useForm } from "react-hook-form";
import { NoticePeriodEnum } from "contract/enum";
import { UserRole } from "contract/enum";
import { useRouter } from "next/router";
import Cookies from "js-cookie";
import { useApi } from "@/hooks/useApi";

const CreateJobSeekerProfileForm = ({
  data,
  setSelectedRole,
}: {
  data: GoogleUserDataType;
  setSelectedRole: (value: UserRole | null) => void;
}) => {
  const router = useRouter();
  const redirectUrl = String(router.query.redirectUrl ?? "");
  const { makeApiCall } = useApi();
  const hForm = useForm<ModifiedJobSeekerUserCreationDataType>({
    mode: "onChange",
    defaultValues: {
      ...defaultJobSeekerCreationData,
      firstName: data.firstName,
      lastName: data.lastName,
      picture: data.picture,
      email: data.email,
    },
  });

  const createProfileData = ({
    payload,
  }: {
    payload: ModifiedJobSeekerUserCreationDataType;
  }) => {
    makeApiCall({
      fetcherFn: async () => {
        const response = await getQueryClient().user.createJobseeker.mutation({
          body: {
            ...payload,
            externalLink: payload?.externalLink ?? "",
            city: !isNil(payload.city?.value) ? Number(payload.city?.value) : 0,
            experienceInYear: !isNil(payload.experienceInYear)
              ? Number(payload.experienceInYear)
              : 0,
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
            expectedSalaryInLpa: payload.expectedSalaryInLpa
              ? Number(payload.expectedSalaryInLpa)
              : null,
          },
        });
        return response;
      },
      successMsgProps: { message: "Profile created successfully." },
      onSuccessFn: (response) => {
        if (response.status === 200) {
          Cookies.set("userToken", response.body.token);
          if (redirectUrl) {
            router.push(redirectUrl);
            return;
          }
          router.push("/");
        }
      },
      showFailureMsg: true,
    });
  };
  return (
    <JobSeekerProfileForm
      hForm={hForm}
      onSubmit={createProfileData}
      mode={UserCreateModeEnum.create}
      onCancel={() => {
        setSelectedRole(null);
      }}
    />
  );
};
export default CreateJobSeekerProfileForm;
