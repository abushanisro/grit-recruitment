import React from "react";
import { Text, Button } from "@mantine/core";
import classes from "/styles/editButton.module.css";
const PostPaidModalBody = ({
  boostDays,
  handlePromotedJob,
}: {
  boostDays: number | null;
  handlePromotedJob: () => void;
}) => {
  return (
    <>
      <Text maw={520} fz={{ base: 12, sm: 16 }} c="white">
        Once your promote your job post, you cannot undo this action. You job
        post remains boosted for {boostDays} days
      </Text>

      <Button
        className={classes.GoButton}
        onClick={handlePromotedJob}
        style={{ border: "1px solid primarySkyBlue.6", borderRadius: "8px" }}
        fw={600}
        w={{ base: "100%", sm: "48%" }}
        fz={{ base: 14, md: 16 }}
        c="black"
      >
        Agree & Promote now
      </Button>
    </>
  );
};

export default PostPaidModalBody;
