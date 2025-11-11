import React from "react";
import { Box, Button, Stack, Text } from "@mantine/core";

import classes from "/styles/editButton.module.css";
const UnPromotedComponent = ({ handleClick }: { handleClick: () => void }) => {
  return (
    <div>
      <Box w={"100%"} pt={20} pb={30} px={{ base: 10, sm: 40 }}>
        <Stack gap={40}>
          <Stack gap={4}>
            <Text fz={{ base: 20, md: 28 }} fw={700} c="white">
              Explore plan
            </Text>
            <Text fz={{ base: 12, sm: 16 }} c="white">
              Promote your job post and get more visibility to your job listing
            </Text>
          </Stack>
          <Stack gap={20}>
            <Text fz={{ base: 10, sm: 14 }} c="white">
              You currently do not have any active subscriptions to promote your
              job post
            </Text>
            <Box>
              <Button
                className={classes.GoButton}
                onClick={handleClick}
                bg="primarySkyBlue.6"
                style={{
                  border: "1px solid primarySkyBlue.6",
                  borderRadius: "8px",
                }}
                fw={600}
                w={{ base: "100%", sm: "70%" }}
                fz={{ base: 12, md: 16 }}
                c="black"
              >
                Explore Premium job post pricing
              </Button>
            </Box>
          </Stack>
        </Stack>
      </Box>
    </div>
  );
};

export default UnPromotedComponent;
