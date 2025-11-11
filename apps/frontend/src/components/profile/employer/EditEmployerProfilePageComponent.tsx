import { useForm } from "react-hook-form";
import { UserCreateModeEnum } from "@/types/jobSeeker";
import { getQueryClient } from "api";
import CustomSkeleton from "@components/CustomSkeleton";
import { useEffect } from "react";
import isNil from "lodash/isNil";
import { useRouter } from "next/router";
import { useApi } from "@/hooks/useApi";
import { contract } from "contract";
import { useQueryClient } from "@tanstack/react-query";
import EmployerProfileForm, {
  defaultEmployerData,
  getCompanySizeLabel,
} from "@/components/profile/employer/EmployerProfileForm";
import {
  EmployerUserDataType,
  ModifiedEmployerUserCreationDataType,
} from "@/types/employer";
import isEmpty from "lodash/isEmpty";
import ErrorMessage from "@components/ErrorMessage";
import { CompanySizeEnum, UserRole } from "contract/enum";

const EditEmployerProfilePageComponent = () => {
  const { data, isLoading, error } =
    getQueryClient().user.getEmployerById.useQuery(
      [contract.user.getEmployerById.path],
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

const EditProfileComponent = ({ data }: { data: EmployerUserDataType }) => {
  const router = useRouter();
  const { makeApiCall } = useApi();
  const mutateQueryClient = useQueryClient();
  const hForm = useForm<ModifiedEmployerUserCreationDataType>({
    mode: "onChange",
    defaultValues: defaultEmployerData,
  });
  const { reset } = hForm;
  const updateProfileData = ({
    payload,
  }: {
    payload: ModifiedEmployerUserCreationDataType;
  }) => {
    makeApiCall({
      fetcherFn: async () => {
        const response = await getQueryClient().user.updateEmployer.mutation({
          body: {
            ...payload,
            picture: payload.company.logo,
            city: !isNil(payload.city?.value)
              ? Number(payload.city?.value)
              : 0,
            company: {
              ...payload.company,
              industries:
                payload.company.industries?.map((eachValue) =>
                  Number(eachValue.value)
                ) ?? [],
            },
            companySize: payload.companySize?.value
              ? (payload.companySize?.value as CompanySizeEnum)
              : null,
            aboutCompany: !isEmpty(payload.aboutCompany)
              ? payload.aboutCompany
              : "",
          },
        });
        return response;
      },
      successMsgProps: { message: "Profile saved successfully" },
      onSuccessFn: () => {
        mutateQueryClient.invalidateQueries({
          queryKey: [contract.user.getEmployerById.path],
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
      role: UserRole.employer,
      company: {
        ...data.company,
        industries: data.company.industries.map((eachIndustry) => ({
          label: eachIndustry.name,
          value: String(eachIndustry.id),
        })),
      },
      aboutCompany: !isEmpty(data.aboutCompany) ? data.aboutCompany : "",
      companySize: data.companySize
        ? {
            label: getCompanySizeLabel(data.companySize),
            value: data.companySize,
          }
        : null,
      city: data.city
        ? {
            label: data.city.name,
            value: String(data.city.id),
          }
        : null,
    });
  }, [data]);
  return (
    <EmployerProfileForm
      hForm={hForm}
      onSubmit={updateProfileData}
      mode={UserCreateModeEnum.edit}
      onCancel={() => {
        router.push("/profile");
      }}
    />
  );
};

export default EditEmployerProfilePageComponent;
