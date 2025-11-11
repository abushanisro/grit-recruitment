import { Divider, Grid, Group, Text } from "@mantine/core";

const InlineLabelWithInputField = ({
  label,
  isRequired = false,
  children,
}: {
  label: string;
  isRequired?: boolean;
  children: React.ReactNode;
}) => {
  return (
    <Grid style={{ overflow: "visible" }} align="center" w="100%">
      <Grid.Col span={{ base: 12, md: "content" }}>
        <Group>
          <Text c="secondaryGreen.1" fw={700} fz={17}>
            {label}{" "}
            {isRequired && (
              <Text span c="red">
                *
              </Text>
            )}
          </Text>
          <Divider
            display={{ base: "none", md: "block" }}
            h={30}
            w={2}
            orientation="vertical"
            bg="secondaryGreen.1"
          />
        </Group>
      </Grid.Col>
      <Grid.Col span={{ base: 12, md: "auto" }} w="100%">
        {children}
      </Grid.Col>
    </Grid>
  );
};
export default InlineLabelWithInputField;
