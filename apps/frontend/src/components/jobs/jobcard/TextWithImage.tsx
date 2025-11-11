import { Group, Image, Text } from "@mantine/core";

const TextWithImage = ({
  imageUrl,
  text,
}: {
  imageUrl: string;
  text: string;
}) => {
  return (
    <Group wrap="nowrap" gap={10}>
      <Image src={imageUrl} alt="" sizes="20px" />
      <Text fz={{ base: 14, md: 18 }} lh="1.16" c="white">
        {text}
      </Text>
    </Group>
  );
};
export default TextWithImage;
