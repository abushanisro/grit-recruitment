import { FaqQuestionAnswerInterface } from "@/types/employer";
import { Accordion, Box, Text } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { Fragment } from "react";
import AccordionItemComponent from "@components/employer/AccordianItemComponent";

const FAQ = ({
  faqQuestionAndAnswers,
}: {
  faqQuestionAndAnswers: FaqQuestionAnswerInterface[];
}) => {
  const isTablet = useMediaQuery("(max-width: 992px)");
  return (
    <Box w="100%" maw={1600} mx="auto">
      <Text
        fz={{ base: 32, md: 40 }}
        fw={700}
        lh="1.17"
        lts={2}
        c="white"
        ta="center"
        mb={40}
        mt={isTablet ? 0 : 40}
      >
        Frequently asked questions
      </Text>
      <Accordion
        styles={{
          root: {
            borderTop: "1px solid",
            borderColor: "var(--mantine-color-secondaryGreen-1)",
          },
        }}
      >
        {faqQuestionAndAnswers.map((question, index) => {
          return (
            <Fragment key={index}>
              <AccordionItemComponent question={question} />
            </Fragment>
          );
        })}
      </Accordion>
    </Box>
  );
};
export default FAQ;
