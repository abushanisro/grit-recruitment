import { Group, Divider, Text } from "@mantine/core";
import TextWithImage from "@components/jobs/jobcard/TextWithImage";
import { ExpertiseLevelEnum } from "contract/enum";

const FreelanceLocationAndExpertise = ({
  location,
  expertise,
}: {
  location: string;
  expertise: ExpertiseLevelEnum;
}) => {
  return (
    <Group gap={10}>
      <TextWithImage imageUrl="/images/coloredLocation.svg" text={location} />
      <Divider orientation="vertical" w={1} />
      <Text
        fz={{ base: 14, md: 18 }}
        lh="1.16"
        c="white"
        tt="uppercase"
        fw={500}
        lts={3}
      >
        {expertise}
      </Text>
    </Group>
  );
};

export default FreelanceLocationAndExpertise;
