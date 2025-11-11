import SectionContainer from "@components/SectionContainer";
import CustomBadge from "@components/jobs/CustomBadge";
import YellowText from "@/components/YellowText";
import {
  Box,
  Center,
  Flex,
  Image,
  Input,
  Modal,
  SimpleGrid,
  Stack,
  Text,
} from "@mantine/core";
import { UserRole } from "contract/enum";
import PrimaryButton from "@components/buttons/PrimaryButton";
import { useDisclosure, useMediaQuery } from "@mantine/hooks";
import { GoogleUserDataType } from "@/types/auth";
import CreateJobSeekerProfileForm from "@components/signup/CreateJobSeekerProfileForm";
import ComingSoonComponent from "@components/ComingSoonComponent";
import { useQueryState } from "@/hooks/queryState";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import CreateEmployerProfileForm from "@components/signup/CreateEmployerProfileForm";
import { getLabelForUserRoleEnum } from "@/data/labelForUserRole";

const roles = Object.values(UserRole).filter((role) => role !== UserRole.admin);
const signUpToastId = "signUpToastId";
const getProfileForm = ({
  role,
  data,
  setSelectedRole,
}: {
  role: UserRole;
  data: GoogleUserDataType;
  setSelectedRole: (value: UserRole | null) => void;
}) => {
  switch (role) {
    case UserRole.jobSeeker:
      return (
        <CreateJobSeekerProfileForm
          data={data}
          setSelectedRole={setSelectedRole}
        />
      );
    case UserRole.employer:
      return (
        <CreateEmployerProfileForm
          data={data}
          setSelectedRole={setSelectedRole}
        />
      );

    case UserRole.admin:
    default:
      return <ComingSoonComponent />;
  }
};

const SignUpFormPage = ({ data }: { data: GoogleUserDataType }) => {
  const [opened, { close }] = useDisclosure(true);

  const [selectedRole, setSelectedRole] = useQueryState<UserRole | null>(
    "selectedRole",
    null
  );

  return (
    <>
      <Modal
        opened={opened || !selectedRole}
        onClose={close}
        size="auto"
        h="auto"
        overlayProps={{
          backgroundOpacity: 0.55,
          blur: 3,
        }}
        styles={{
          header: {
            background: "transparent",
            color: "white",
          },
          content: { background: "transparent" },
        }}
        centered
        closeOnEscape={false}
        closeOnClickOutside={false}
        withCloseButton={false}
      >
        <Center>
          <SignupForm
            data={data}
            onClose={close}
            selectedRole={selectedRole}
            setSelectedRole={setSelectedRole}
          />
        </Center>
      </Modal>
      {selectedRole && (
        <>{getProfileForm({ role: selectedRole, data, setSelectedRole })}</>
      )}
    </>
  );
};

const SignupForm = ({
  data,
  onClose,
  selectedRole,
  setSelectedRole,
}: {
  data: GoogleUserDataType;
  onClose: () => void;
  selectedRole: UserRole | null;
  setSelectedRole: (value: UserRole | null) => void;
}) => {
  const { showToast } = useCustomToast();
  const tablet = useMediaQuery("(max-width: 992px)");
  const handleSubmit = () => {
    if (!selectedRole) {
      showToast({
        status: ToastStatus.error,
        id: signUpToastId,
        message: "Please select a role",
      });
      return;
    }
    onClose();
  };
  return (
    <SectionContainer maw={1100} p={0} h="max-content">
      <SimpleGrid p={0} cols={{ base: 1, sm: 2 }}>
        <Image
          src="/images/signUp.svg"
          alt="signUp"
          h="100%"
          fit="cover"
          display={{ base: "none", sm: "block" }}
        />

        <Stack p={20} gap={0}>
          <Text
            fw="700"
            fz={{ base: 20, sm: 24, lg: 32 }}
            pb={30}
            c="white"
            ta="center"
          >
            Sign Up!
          </Text>
          <Stack gap={35}>
            <Box>
              <YellowText
                label="Create account as a"
                fz={{ base: 16, sm: 18, lg: 20 }}
                pb={10}
                fw="700"
              />
              <Flex gap={10} wrap="wrap">
                {roles.map((role, index) => {
                  const isActive = role === selectedRole;
                  return (
                    <Box
                      key={index}
                      onClick={() => {
                        setSelectedRole(role);
                      }}
                    >
                      <CustomBadge
                        label={getLabelForUserRoleEnum(role)}
                        bg={
                          isActive
                            ? "var(--mantine-color-primarySkyBlue-6)"
                            : "transparent"
                        }
                        c={isActive ? "black" : "white"}
                        px={16}
                        py={16}
                        fw="600"
                        tt="capitalize"
                        fz={{ base: 14, md: 16 }}
                        style={{
                          cursor: "pointer",
                          border: "0.6px solid",
                          borderColor: "var(--mantine-color-primaryGrey-1)",
                        }}
                      />
                    </Box>
                  );
                })}
              </Flex>
            </Box>
            <Box>
              <Box>
                <YellowText
                  label="Your Name"
                  fz={{ base: 16, sm: 18, lg: 20 }}
                  pb={10}
                  fw="700"
                />
                <Input
                  styles={{
                    input: {
                      border: "0.6px solid",
                      borderColor: "white",
                      borderRadius: 19,
                      color: "white",
                      backgroundColor: "transparent",
                      fontSize: tablet ? 14 : 16,
                      paddingInline: 20,
                      paddingBlock: 10,
                    },
                  }}
                  placeholder="Name"
                  rightSectionPointerEvents="all"
                  maw={450}
                  value={data.firstName}
                  disabled
                />
              </Box>
              <Box mt={15}>
                <YellowText
                  label="Your email"
                  fz={{ base: 16, sm: 18, lg: 20 }}
                  pb={10}
                  fw="700"
                />
                <Input
                  styles={{
                    input: {
                      border: "0.6px solid",
                      borderColor: "white",
                      borderRadius: 19,
                      color: "white",
                      backgroundColor: "transparent",
                      fontSize: tablet ? 14 : 16,
                      paddingInline: 20,
                      paddingBlock: 10,
                    },
                  }}
                  placeholder="Email"
                  rightSectionPointerEvents="all"
                  maw={450}
                  disabled
                  value={data.email}
                />
              </Box>
            </Box>

            <PrimaryButton
              label="Let's Go"
              maw={{ base: "100%", sm: "max-content" }}
              fz={{ base: 16, sm: 20 }}
              fw="600"
              px={20}
              py={12}
              h="max-content"
              onClick={handleSubmit}
            />
          </Stack>
        </Stack>
      </SimpleGrid>
    </SectionContainer>
  );
};
export default SignUpFormPage;
