import { Avatar, Group, Stack, Text, Divider, Box, Badge } from "@mantine/core";
import {
  JobSeekerDirectoryPaginatedListType,
  jobseekerDirectoryType,
} from "@/types/employer";
import YellowText from "@components/YellowText";
import { useMediaQuery } from "@mantine/hooks";
import { useHover } from "@mantine/hooks";
import CustomToolTip from "@components/CustomToolTip";
import { getQueryClient } from "api";
import { useApi } from "@/hooks/useApi";
import { useRouter } from "next/router";
import { IconEye, IconMail } from "@tabler/icons-react";
import { useLogin } from "@/contexts/LoginProvider";
import EmployerOnlyInviteButton from "@components/buttons/EmployerOnlyInviteButton";
import CommonCheckbox from "@components/CommonCheckbox";

const JobSeekerCard = ({
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
  const isMobile = useMediaQuery("(max-width: 426px)");
  const { isLoggedIn } = useLogin();
  const { makeApiCall } = useApi();
  const router = useRouter();
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
      setJobSeekersId(
        jobSeekersId.filter((currentId: number) => currentId !== id)
      );
    } else {
      setJobSeekersId([...jobSeekersId, id]);
    }
  };

  const { hovered, ref } = useHover();
  return (
    <Stack
      ref={ref}
      pos="relative"
      bg={
        hovered
          ? "transparent"
          : "linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
      }
      mx="auto"
      onClick={() => handleViewJobseeker(data)}
      w="100%"
      px={20}
      py={20}
      maw={900}
      gap={20}
      h="100%"
      style={{
        borderRadius: isMobile ? 10 : 38,
        border: "1px solid",
        borderColor: "var(--mantine-color-secondaryGreen-1)",
        boxShadow: "0px 1px 4px 0px var(--mantine-color-primaryGreen-4)",
        cursor: "pointer",
      }}
    >
      <Group gap={20}>
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
        <Stack gap="4">
          <Box
            onClick={(e) => {
              e.stopPropagation();
              e.preventDefault();
            }}
          >
            <CustomToolTip label={data.fullName}>
              <Text
                tt="uppercase"
                c="white"
                fw={700}
                lh="1.17"
                maw="120"
                lts={1.1}
                fz={{ base: 14, sm: 18 }}
                mb={{ base: 10, xl: 12 }}
                lineClamp={2}
              >
                {data.fullName}
              </Text>
            </CustomToolTip>
          </Box>
          <YellowText
            tt="capitalize"
            fw={600}
            maw="120"
            lineClamp={2}
            fz={{ base: 14, sm: 16 }}
            label={data.headline}
          />
        </Stack>
      </Group>
      <Box
        onClick={(e) => {
          e.stopPropagation();
        }}
      >
        <CommonCheckbox
          c="primaryOrange.5"
          checked={jobSeekersId.includes(data.id)}
          onChange={() => handleCheckboxChange(data.id)}
          pos="absolute"
          top={20}
          right={20}
          bg="black"
        />
        <Stack gap={2} pos="absolute" top={50} right={18}>
          {data.hasViewed && (
            <CustomToolTip label={` Viewed from ${data.viewedUnderPlan} plan`}>
              <Group fw={700} gap={6}>
                <IconEye color="var(--mantine-color-secondaryGreen-1)" />
              </Group>
            </CustomToolTip>
          )}
          {data.hasInvited && (
            <CustomToolTip label={"Invited Candidate"}>
              <Group fw={700} gap={6}>
                <IconMail color="var(--mantine-color-secondaryGreen-1)" />
              </Group>
            </CustomToolTip>
          )}
        </Stack>
      </Box>
      <Stack gap={12}>
        <Group>
          <Text fz={{ base: 14 }} c="white">
            Location
          </Text>
          <Divider c="white" orientation="vertical" />
          <Text fz={{ base: 14 }} fw={700} c="white">
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
          <Group wrap={"wrap"}>
            <Text fz={{ base: 14 }} c="white">
              Skills
            </Text>
            <Divider c="white" orientation="vertical" />
            <Group gap="8" maw="200">
              {data.skills.map((skill, index) => (
                <Group wrap={"wrap"} key={index}>
                  <Badge fz={10} color=" rgba(255, 255, 255, 0.15)">
                    {skill}
                  </Badge>
                </Group>
              ))}
            </Group>
          </Group>
        )}
      </Stack>
      <Stack>
        {isLoggedIn && (
          <EmployerOnlyInviteButton
            onEmployerSendInvite={() => {
              setJobSeekersId([data.id]);
              onEmailInviteOpen();
            }}
            w={"100%"}
          />
        )}
      </Stack>
    </Stack>
  );
};
export default JobSeekerCard;
