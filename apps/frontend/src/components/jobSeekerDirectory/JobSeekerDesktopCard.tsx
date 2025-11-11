import {
  Avatar,
  Group,
  Stack,
  Flex,
  Box,
  Text,
  Checkbox,
  Divider,
  Badge,
} from "@mantine/core";
import { IconEye, IconMail } from "@tabler/icons-react";
import {
  JobSeekerDirectoryPaginatedListType,
  jobseekerDirectoryType,
} from "@/types/employer";
import YellowText from "@components/YellowText";
import { useMediaQuery } from "@mantine/hooks";
import { useHover } from "@mantine/hooks";
import { getQueryClient } from "api";
import { useApi } from "@/hooks/useApi";
import { useRouter } from "next/router";
import { useLogin } from "@/contexts/LoginProvider";
import EmployerOnlyInviteButton from "@components/buttons/EmployerOnlyInviteButton";

const JobSeekerDesktopCard = ({
  data,
  setJobSeekersId,
  onEmailInviteOpen,
  onRenewSubscriptionOpen,
  jobSeekersId,
  subscriptionId,
}: {
  data: JobSeekerDirectoryPaginatedListType["results"][number];

  onEmailInviteOpen: () => void;
  onRenewSubscriptionOpen: () => void;
  setJobSeekersId: (value: number[]) => void;
  jobSeekersId: number[];
  subscriptionId: string;
}) => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  const { makeApiCall } = useApi();
  const router = useRouter();
  const { hovered, ref } = useHover();
  const { isLoggedIn } = useLogin();
  const handleViewJobseeker = (data: jobseekerDirectoryType) => {
    const queryObj = {
      jobSeekerId: String(data && data.id),
      subscriptionId: String(subscriptionId),
    };
    if (data) {
      makeApiCall({
        fetcherFn: async () => {
          const response =
            await getQueryClient().subscription.getEmployerProfileView.mutation(
              {
                body: queryObj,
              }
            );
          return response;
        },
        onSuccessFn: (response) => {
          if (response.status == 201) {
            return router.push(`/jobseeker/${response.body.viewId}`);
          }
        },
        onFailureFn: () => {
          onRenewSubscriptionOpen();
        },
        showFailureMsg: false,
      });
    }
  };

  const handleCheckboxChange = (id: number) => {
    if (jobSeekersId.includes(id)) {
      // If the id is already in the array, remove it
      setJobSeekersId(
        jobSeekersId.filter((currentId: number) => currentId !== id)
      );
    } else {
      // If the id is not in the array, add it
      setJobSeekersId([...jobSeekersId, id]);
    }
  };
  return (
    <Flex
      ref={ref}
      onClick={() => handleViewJobseeker(data)}
      pos="relative"
      bg={
        hovered
          ? "transparent"
          : "linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
      }
      w="100%"
      px={{ base: 40, md: 60 }}
      py={40}
      justify="space-between"
      h="100%"
      style={{
        borderRadius: isMobile ? 10 : 38,
        border: "1px solid",
        cursor: "pointer",
        borderColor: "var(--mantine-color-secondaryGreen-1)",
        boxShadow: hovered
          ? "0px 4px 4px 0px var(--mantine-color-primaryGreen-4)"
          : "0px 0px 0px 0px var(--mantine-color-primaryGreen-4)",
      }}
    >
      <Stack gap={30}>
        <Flex align="center" gap="30">
          <Box
            onClick={(e) => {
              e.stopPropagation();
            }}
          >
            <Checkbox
              c="rimarySkyBlue.6"
              fz={{ base: 16, md: 20 }}
              checked={jobSeekersId.includes(data.id)}
              onChange={() => handleCheckboxChange(data.id)}
              styles={{
                root: { alignItems: "center" },
                label: {
                  userSelect: "none",
                  fontSize: 20,
                  color: "var(--mantine-color-primarySkyBlue-6)",
                },
                input: {
                  borderColor: "var(--mantine-color-primarySkyBlue-6)",
                  backgroundColor: "var(--mantine-color-primaryPaleBlue-9)",
                },
              }}
            />
          </Box>

          <Stack>
            <Flex gap={32}>
              <Box>
                <Avatar
                  w={90}
                  h={90}
                  bg="customGray.2"
                  style={{
                    border: "1px solid",
                    borderColor: "var(--mantine-color-secondaryGreen-1)",
                  }}
                  src={data.picture ?? ""}
                  alt="profile pic"
                />
              </Box>

              <Stack gap={12}>
                <Text
                  pt={12}
                  tt="uppercase"
                  c="white"
                  fw={700}
                  lh="1.17"
                  lts={1.1}
                  fz={{ base: 18, md: 24 }}
                  lineClamp={3}
                  style={{ wordBreak: "break-word" }}
                >
                  {data.fullName}
                </Text>
                <YellowText
                  tt="capitalize"
                  fw={600}
                  fz={{ base: 16, md: 20 }}
                  label={data.headline}
                />
              </Stack>
            </Flex>
            <Group>
              <Text c="white">Location</Text>
              <Divider c="white" orientation="vertical" />
              <Text fw={700} c="white">
                {data.city}
              </Text>
            </Group>
            <Group>
              <Text fz={{ base: 14 }} c="white">
                Experience
              </Text>
              <Divider c="white" orientation="vertical" />
              {data.experienceInYears === null ? (
                <>-</>
              ) : data.experienceInYears == "0" ? (
                <Text fz={{ base: 14 }} fw={700} c="white">
                  Fresher
                </Text>
              ) : (
                <Text fz={{ base: 14 }} fw={700} c="white">
                  {data.experienceInYears} years
                </Text>
              )}
            </Group>
            {data.expectedSalaryInLpa && (
              <Group>
                <Text fz={{ base: 14 }} c="white">
                  Expected Salary
                </Text>
                <Divider c="white" orientation="vertical" />
                {data.experienceInYears ? (
                  <Text fz={{ base: 14 }} fw={700} c="white">
                    {data.expectedSalaryInLpa} LPA
                  </Text>
                ) : (
                  <>-</>
                )}
              </Group>
            )}
            {data.skills && (
              <Group>
                <Text c="white">Skills</Text>
                <Divider c="white" orientation="vertical" />

                {data.skills.map((skill, index) => (
                  <Group wrap={"wrap"} key={index}>
                    <Badge p="10" color=" rgba(255, 255, 255, 0.15)">
                      {skill}
                    </Badge>
                  </Group>
                ))}
              </Group>
            )}
          </Stack>
        </Flex>
      </Stack>

      <Group py={13} justify="center" miw={{ base: 245, md: 245 }}>
        <Stack align="center">
          {data.hasInvited && (
            <Group fw={700} gap={6}>
              <IconMail color="var(--mantine-color-secondaryGreen-1)" />
              <Text
                fz={{ base: 14, md: 16 }}
                fw={600}
                c="var(--mantine-color-secondaryGreen-1)"
              >
                Invited Candidate
              </Text>{" "}
            </Group>
          )}
          {data.hasViewed ? (
            <Group fw={700} gap={6}>
              <IconEye color="var(--mantine-color-secondaryGreen-1)" />
              <Text
                fz={{ base: 14, md: 16 }}
                fw={600}
                c="var(--mantine-color-secondaryGreen-1)"
              >
                Viewed from {data.viewedUnderPlan} plan
              </Text>{" "}
            </Group>
          ) : (
            <></>
          )}
          {isLoggedIn && (
            <EmployerOnlyInviteButton
              onEmployerSendInvite={() => {
                setJobSeekersId([data.id]);
                onEmailInviteOpen();
              }}
            />
          )}
        </Stack>
      </Group>
    </Flex>
  );
};
export default JobSeekerDesktopCard;
