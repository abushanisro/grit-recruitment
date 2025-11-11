import { GoogleUserDataType } from "@/types/auth";
import { UserCreateModeEnum } from "@/types/jobSeeker";
import { getQueryClient } from "api";
import { useForm } from "react-hook-form";
import { CompanySizeEnum, UserRole } from "contract/enum";
import { useRouter } from "next/router";
import Cookies from "js-cookie";
import { useApi } from "@/hooks/useApi";
import EmployerProfileForm, {
  defaultEmployerData,
} from "@components/profile/employer/EmployerProfileForm";
import { ModifiedEmployerUserCreationDataType } from "@/types/employer";
import { convertOptionsToNumberList } from "@/utils/form";
import isEmpty from "lodash/isEmpty";

const CreateEmployerProfileForm = ({
  data,
  setSelectedRole,
}: {
  data: GoogleUserDataType;
  setSelectedRole: (value: UserRole | null) => void;
}) => {
  const router = useRouter();
  const redirectUrl = String(router.query.redirectUrl ?? "");
  const { makeApiCall } = useApi();
  const hForm = useForm<ModifiedEmployerUserCreationDataType>({
    mode: "onChange",
    defaultValues: {
      ...defaultEmployerData,
      firstName: data.firstName,
      lastName: data.lastName,
      company: { logo: data.picture },
      email: data.email,
    },
  });

  const createProfileData = ({
    payload,
  }: {
    payload: ModifiedEmployerUserCreationDataType;
  }) => {
    makeApiCall({
      fetcherFn: async () => {
        const response = await getQueryClient().user.createEmployer.mutation({
          body: {
            ...payload,
            picture: payload.company.logo,
            company: {
              ...payload.company,
              website: payload.company.website ?? null,
              industries: payload.company.industries
                ? convertOptionsToNumberList(payload.company.industries)
                : [],
            },
            city: payload.city ? Number(payload.city.value) : 0,   // City is a mandatory field. The fallback value of 0 is set to bypass type checking,
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
      successMsgProps: { message: "Profile created successfully." },
      onSuccessFn: (response) => {
        if (response.status === 200) {
          Cookies.set("userToken", response.body.token);
          if (redirectUrl) {
            router.push(redirectUrl);
            return;
          }
          router.push("/employer");
        }
      },
      showFailureMsg: true,
    });
  };
  return (
    <EmployerProfileForm
      hForm={hForm}
      onSubmit={createProfileData}
      mode={UserCreateModeEnum.create}
      onCancel={() => {
        setSelectedRole(null);
      }}
    />
  );
};
export default CreateEmployerProfileForm;
