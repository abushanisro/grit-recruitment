import { FaqQuestionAnswerInterface } from "@/types/employer";
import classes from "styles/faq.module.css";
import { Accordion, AccordionItem, AccordionPanel, Text } from "@mantine/core";
import { IconPlus } from "@tabler/icons-react";

const AccordionItemComponent = ({
  question,
}: {
  question: FaqQuestionAnswerInterface;
}) => {
  return (
    <AccordionItem
      value={question.id}
      style={{
        borderBottom: "1px solid",
        borderColor: "var(--mantine-color-secondaryGreen-1)",
      }}
    >
      <Accordion.Control
        className={classes.faq}
        c="white"
        chevron={<IconPlus size={20} />}
        styles={{
          chevron: {
            width: 20,
            height: 20,
          },
        }}
      >
        <Text fz={{ base: 16, md: 24 }} fw={600} lh="1.5" lts={2}>
          {question.question}
        </Text>
      </Accordion.Control>
      <AccordionPanel>
        <Text fz={{ base: 14, md: 18 }} lh="1.45" c="customGray.2">
          {question.answer}
        </Text>
      </AccordionPanel>
    </AccordionItem>
  );
};
export default AccordionItemComponent;
