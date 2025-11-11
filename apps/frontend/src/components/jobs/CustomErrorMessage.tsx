import { Center, Text } from "@mantine/core";

const CustomErrorMessage = ({
  errorMessage = "An error occurred",
}: {
  errorMessage?: string;
}) => {
  return (
    <Center bg="customBlack.4" p={10} h="100%">
      <Text c="white">{errorMessage}</Text>
    </Center>
  );
};
export default CustomErrorMessage;
