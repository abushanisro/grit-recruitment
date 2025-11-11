import { Box } from "@mantine/core";
import React from "react";
import AboutDatapage from "./AboutDatapage";
import CustomContainer from "@components/CustomContainer";

const AboutPage = () => {
  return (
    <Box mt={50}>
      <CustomContainer>
        <AboutDatapage />
      </CustomContainer>
    </Box>
  );
};

export default AboutPage;
