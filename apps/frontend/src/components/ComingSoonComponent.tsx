import { Flex, Text } from "@mantine/core";
import PrimaryButton from "./buttons/PrimaryButton";
import Link from "next/link";

const ComingSoonComponent = () => {
  return (
    <Flex
      h="70vh"
      style={{ flexDirection: "column" }}
      justify="center"
      align="center"
      gap={10}
    >
      <Text>Work in progress</Text>
      <Link href="/">
        <PrimaryButton label="Go to homepage" />
      </Link>
    </Flex>
  );
};
export default ComingSoonComponent;
