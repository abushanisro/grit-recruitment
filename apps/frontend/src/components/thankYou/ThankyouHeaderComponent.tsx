import { Flex } from "@mantine/core";
import { IconCircleCheckFilled } from "@tabler/icons-react";
import React from "react";

const ThankyouHeaderComponent = () => {
  return (
    <Flex justify="center">
      <Flex
        justify="center"
        align="center"
        style={{ borderRadius: "50%" }}
        bg={"#25282F"}
        w={50}
        h={50}
      >
        <IconCircleCheckFilled size={30} style={{ color: " #48ff68" }} />
      </Flex>
    </Flex>
  );
};

export default ThankyouHeaderComponent;
