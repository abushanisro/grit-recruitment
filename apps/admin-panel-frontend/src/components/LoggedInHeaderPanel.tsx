import ProfileMenuButton from "@/components/ProfileMenuButton";
import { Tooltip } from "@mantine/core";
import { IconAlertCircleFilled } from "@tabler/icons-react";
import PrimaryIconButton from "@/components/buttons/PrimaryIconButton";
import { useUserData } from "@/contexts/UserProvider";

const LoggedInHeaderPanel = () => {
  const { userDetails } = useUserData();
  if (!userDetails) {
    return (
      <PrimaryIconButton>
        <Tooltip label="Profile data not found">
          <IconAlertCircleFilled
            size={32}
            color="black"
            aria-label="profile icon not found"
          />
        </Tooltip>
      </PrimaryIconButton>
    );
  }

  return <ProfileMenuButton profilePicture={userDetails.picture ?? null} />;
};
export default LoggedInHeaderPanel;
