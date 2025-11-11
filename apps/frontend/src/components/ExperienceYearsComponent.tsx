import React from "react";
import { Text } from "@mantine/core";
import { ExperienceInYear } from "@/types/jobSeeker";
const formatExperienceRange = (years: ExperienceInYear) => {
  if (years.minExp === 0 && years.maxExp === 0) {
    return "Fresher";
  }
  return `${years.minExp}-${years.maxExp} Years`;
};
const ExperienceYearsComponent = ({ years }: { years: ExperienceInYear }) => {
  return (
    <Text fz={{ base: 12, sm: 14, md: 16, xl: 18 }} lh="1.17" c="white">
      Years of Experience:{" "}
      <Text span fw={700} fz="inherit">
        {formatExperienceRange(years)}
      </Text>
    </Text>
  );
};

export default ExperienceYearsComponent;
