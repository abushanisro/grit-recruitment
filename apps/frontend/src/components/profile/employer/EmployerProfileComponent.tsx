import { Avatar, Box, Flex, Space } from "@mantine/core";
import SectionContainer from "@components/SectionContainer";
import SecondaryButton from "@components/buttons/SecondaryButton";
import Link from "next/link";
import { getQueryClient } from "api";
import { contract } from "contract";
import ProfileLoader from "@components/loaders/ProfileLoader";
import ProfileHeader from "@/components/profile/employer/ProfileHeader";
import ProfileBody from "@/components/profile/employer/ProfileBody";
import ErrorMessage from "@components/ErrorMessage";
import classes from "styles/editProfile.module.css";

const EmployerProfilePageComponent = () => {
  const { data, isLoading, error } =
    getQueryClient().user.getEmployerById.useQuery(
      [contract.user.getEmployerById.path],
      {}
    );
  if (isLoading) {
    return <ProfileLoader />;
  }
  if (error) {
    return <ErrorMessage error={error} />;
  }
  return (
    <Box mt={{ base: 40, sm: 100 }} mb={40}>
      <Flex justify={"flex-end"} pb={10} mr={{ base: 10, md: 20 }}>
        <Link href="/editProfile">
          <SecondaryButton
            label="Edit profile"
            styles={{
              root: {
                border: "1px solid",
                borderColor: "var(--mantine-color-secondaryGreen-1)",
              },
            }}
            className={classes.editProfile}
            c="secondaryGreen.1"
          />
        </Link>
      </Flex>
      <SectionContainer
        p={{ base: 20, md: 32, lg: 40, xl: 60 }}
        h="auto"
        mih="fit-content"
        style={{ overflow: "visible" }}
      >
        <>
          <Avatar
            w={{ base: 100, md: 140, xl: 160 }}
            h={{ base: 100, md: 140, xl: 160 }}
            pos="absolute"
            left={{
              base: "30px",
              sm: "60px",
            }}
            top={{
              base: "-50px",
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
            src={data.body.company.logo ?? ""}
            alt="company logo"
          />

          <ProfileHeader data={data.body} />
          <Space h={{ base: 40, sm: 60 }} />
          <ProfileBody data={data.body} />
        </>
      </SectionContainer>
    </Box>
  );
};
export default EmployerProfilePageComponent;
