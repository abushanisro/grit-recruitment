import SectionContainer from "@components/SectionContainer";
import { Grid, Group } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";

const ProfileFormContainer = ({
  sideLabelComponent,
  formComponent,
}: {
  sideLabelComponent: React.ReactNode;
  formComponent: React.ReactNode;
}) => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  return (
    <Grid
      mt={{ base: 40, sm: 100 }}
      mb={40}
      style={{ overflow: "visible" }}
      gutter={0}
      ml={0}
    >
      <Grid.Col
        span={{ base: 12, sm: 0.5 }}
        pl={{ base: 100, xs: 60, sm: 0 }}
        pr={{ base: 0, sm: 10 }}
      >
        <Group
          style={{
            flexWrap: "nowrap",
            transform: isMobile
              ? "none"
              : "translate(50%, 800%) translateX(-10px) translateY(10px)  rotate(270deg)",
          }}
          maw="fit-content"
        >
          {sideLabelComponent}
        </Group>
      </Grid.Col>
      <Grid.Col span={{ base: 12, sm: 11.5 }}>
        <SectionContainer
          pl={{ base: 20, sm: 60 }}
          py={{ base: 30, sm: 54 }}
          pr={{ base: 20, sm: 54 }}
          h="auto"
          mih="fit-content"
          style={{ overflow: "visible" }}
        >
          {formComponent}
        </SectionContainer>
      </Grid.Col>
    </Grid>
  );
};
export default ProfileFormContainer;
