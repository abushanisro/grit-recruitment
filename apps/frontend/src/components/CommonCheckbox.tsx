import { Checkbox, CheckboxProps } from "@mantine/core";

const CommonCheckbox = ({ ...props }: CheckboxProps) => {
  return (
    <Checkbox
      color="primarySkyBlue.6"
      size="sm"
      fz={{ base: 16, md: 20 }}
      styles={{
        root: { alignItems: "center" },
        label: {
          userSelect: "none",
          fontSize: 20,
          color: "var(--mantine-color-primarySkyBlue-6)",
        },
        input: {
          borderColor: "var(--mantine-color-primarySkyBlue-6)",
          backgroundColor: "var(--mantine-color-primaryPaleBlue-9)",
        },
      }}
      {...props}
    />
  );
};
export default CommonCheckbox;
