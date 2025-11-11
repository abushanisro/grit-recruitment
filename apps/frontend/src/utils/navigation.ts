import { NavigationInterface } from "@/types/navigation";
import { UserRole } from "contract/enum";

export const getHomePageForUserBasedOnRole = (role: string): string => {
  switch (role) {
    case UserRole.jobSeeker:
      return "/jobs";
    case UserRole.employer:
      return "/employer";
    case UserRole.admin:
    default:
      return "/";
  }
};

export const getMenuForUserBasedOnRole = ({
  role,
  pathname,
}: {
  role: UserRole;
  pathname: string;
}): NavigationInterface[] => {
  switch (role) {
    case UserRole.jobSeeker:
      return [
        {
          label: "Discover Jobs",
          pageLink: "/jobs",
          isCurrentPage: pathname === "/jobs",
        },
        {
          label: "My Jobs",
          pageLink: "/myJobs",
          isCurrentPage: pathname === "/myJobs",
        },
      ];
    case UserRole.employer:
      return [
        {
          label: "Post a Job",
          pageLink: "/postJob",
          isCurrentPage: pathname === "/postJob",
        },
        {
          label: "Manage Jobs",
          pageLink: "/manageJobs",
          isCurrentPage: pathname === "/manageJobs",
        },
      ];

    case UserRole.admin:
    default:
      return [
        {
          label: "Discover Jobs",
          pageLink: "/jobs",
          isCurrentPage: pathname === "/jobs",
        },
      ];
  }
};
