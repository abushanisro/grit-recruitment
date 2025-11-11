import { UserRole } from "contract/enum";

export const getLabelForUserRoleEnum = (role: UserRole) => {
  switch (role) {
    case UserRole.jobSeeker:
      return "Jobseeker";
    case UserRole.employer:
      return "Employer";
    default:
      return "";
  }
};






