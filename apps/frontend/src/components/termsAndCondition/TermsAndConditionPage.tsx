import React from "react";
import TermsAndConditionDataPage from "@components/termsAndCondition/TermsAndConditionDataPage";
import { Box } from "@mantine/core";
import CustomContainer from "@components/CustomContainer";

const TermsAndConditionPage = () => {
    
    return (
      <Box mt={40}>
        <CustomContainer>
          <TermsAndConditionDataPage />
        </CustomContainer>
      </Box>
    );
};

export default TermsAndConditionPage;
