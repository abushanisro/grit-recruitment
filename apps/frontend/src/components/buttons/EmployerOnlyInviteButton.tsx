import { ButtonProps } from "@mantine/core";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { useMediaQuery } from "@mantine/hooks";
import { useState } from "react";
import { useUserData } from "@/contexts/UserProvider";
import { UserRole } from "contract/enum";
import { PolymorphicComponentProps } from "@mantine/core/lib/core/factory/create-polymorphic-component";

const EmployerOnlyInviteButton = ({
  onEmployerSendInvite,
  ...props
}: {
  onEmployerSendInvite: () => void;
} & PolymorphicComponentProps<"button", ButtonProps>) => {
  const { userDetails } = useUserData();
  const isEmployer = userDetails?.role === UserRole.employer;
  const [isHovered, setIsHovered] = useState<boolean>(false);
  const isTablet = useMediaQuery("(max-width: 768px)");

  return (
    <>
      {isEmployer && (
        <PrimaryButton
          label="Send Invite"
          onMouseEnter={() => setIsHovered(true)}
          onMouseLeave={() => setIsHovered(false)}
          mx="auto"
          maw={isTablet ? "100%" : 300}
          fw={500}
          bg={isHovered ? "primarySkyBlue.8" : "primarySkyBlue.6"}
          onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            onEmployerSendInvite();
          }}
          {...props}
        />
      )}
    </>
  );
};
export default EmployerOnlyInviteButton;
