import { useRouter } from "next/router";
import { useUserData } from "@/contexts/UserProvider";
import { Tooltip } from "@mantine/core";
import { IconAlertCircleFilled, IconLock } from "@tabler/icons-react";
import PrimaryIconButton from "@/components/buttons/PrimaryIconButton";
import { getMenuForUserBasedOnRole } from "@/utils/navigation";
import { Fragment } from "react";
import NavLinkButton from "@/components/buttons/NavLinkButton";
import PaymentConfirmationModal from "@/components/jobSeekerDirectory/PaymentConfirmationModal";
import { useDisclosure } from "@mantine/hooks";
import { UserRole } from "contract/enum";
import { featureFlags } from "@/utils/featureFlag";

const LoggedInNavbar = () => {
  const { userDetails } = useUserData();
  const router = useRouter();
  const [opened, { close }] = useDisclosure(false);
  const handleClick = () => {
    router.push("/subscription");
    close();
  };

  const navLinkStyles = {
    color: "var(--mantine-color-primarySkyBlue-6)",
    backgroundColor: "var(--mantine-color-primaryDarkBlue-9)",
  };

  if (!userDetails) {
    return (
      <PrimaryIconButton>
        <Tooltip label="Profile data not found">
          <IconAlertCircleFilled
            size={32}
            color="var(--mantine-color-primarySkyBlue-6)"
            aria-label="profile icon not found"
          />
        </Tooltip>
      </PrimaryIconButton>
    );
  }

  if (userDetails.role === UserRole.employer) {
    return (
      <>
        <PaymentConfirmationModal
          opened={opened}
          onClose={close}
          modalHeader={`Please subscribe to our plans to view this page`}
          onSuccessFn={handleClick}
          successButtonLabel="View Plans"
          secondaryButtonLabel="Cancel"
        />
        <Fragment>
          <NavLinkButton
            label={"Post a Job"}
            pageLink={"/postJob"}
            w="100%"
            isCurrentPage={router.pathname === "/postJob"}
            style={navLinkStyles}
          />
          <NavLinkButton
            label={"Manage Jobs"}
            pageLink={"/manageJobs"}
            w="100%"
            isCurrentPage={router.pathname === "/manageJobs"}
            style={navLinkStyles}
          />
          <NavLinkButton
            label={"Orders"}
            pageLink={"/employerOrders"}
            w="100%"
            isCurrentPage={router.pathname === "/employerOrders"}
            style={navLinkStyles}
          />

          <>
            <NavLinkButton
              label={"Jobseeker Directory"}
              pageLink={"/jobseekerDirectoryFilter"}
              w="100%"
              isCurrentPage={router.pathname === "/jobseekerDirectoryFilter"}
              rightSection={
                userDetails &&
                !userDetails.isPurchaseActive && <IconLock size={"18"} />
              }
              style={navLinkStyles}
            />
          </>

          {!featureFlags.unReleased &&
            userDetails &&
            userDetails.role == UserRole.employer &&
            (userDetails.isPurchaseActive ||
              userDetails.isJobBoostPurchaseActive) && (
              <>
                <NavLinkButton
                  label={"Subscription Plans"}
                  pageLink={"/subscription"}
                  w="100%"
                  isCurrentPage={router.pathname === "/subscription"}
                  style={navLinkStyles}
                />
                <NavLinkButton
                  label={"Post Paid Plans"}
                  pageLink={"/postPaidPlans"}
                  w="100%"
                  isCurrentPage={router.pathname === "/postPaidPlans"}
                  style={navLinkStyles}
                />

                <NavLinkButton
                  label={"My Subcriptions"}
                  pageLink={"/subscriptionDetails"}
                  w="100%"
                  isCurrentPage={router.pathname === "/subscriptionDetails"}
                  style={navLinkStyles}
                />
                <NavLinkButton
                  label={"My Payment"}
                  pageLink={"/payment"}
                  w="100%"
                  isCurrentPage={router.pathname === "/payment"}
                  style={navLinkStyles}
                />
              </>
            )}
        </Fragment>
      </>
    );
  }

  return (
    <>
      {getMenuForUserBasedOnRole({
        role: userDetails.role,
        pathname: router.pathname,
      }).map((eachNavObj, index) => {
        return (
          <Fragment key={index}>
            <NavLinkButton
              label={eachNavObj.label}
              pageLink={eachNavObj.pageLink}
              isCurrentPage={eachNavObj.isCurrentPage}
              style={navLinkStyles}
            />
          </Fragment>
        );
      })}
    </>
  );
};

export default LoggedInNavbar;
