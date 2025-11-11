import {
  MantineProvider,
  Checkbox,
  CheckboxProps,
  createTheme,
} from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";

import {
  FieldValues,
  Path,
  RegisterOptions,
  UseFormReturn,
} from "react-hook-form";

const theme = createTheme({
  cursorType: "pointer",
});

function CustomCheckbox<T extends FieldValues>({
  name,
  hForm,
  rules,
  label,
  ...props
}: {
  name: Path<T>;
  hForm: UseFormReturn<T>;
  rules?: RegisterOptions;
  label: string;
} & CheckboxProps) {
  const { register } = hForm;
  const isTablet = useMediaQuery("(max-width: 768px)");
  return (
    <MantineProvider theme={theme}>
      <Checkbox
        color="var(--mantine-color-primaryGrey-1)"
        {...register(name, rules)}
        label={label}
        fz={{ base: 16, md: 20 }}
        styles={{
          root: { alignItems: "center" },
          label: {
            userSelect: "none",
            fontSize: isTablet ? 16 : 20,
            color: "var(--mantine-color-secondaryGreen-1)",
          },
          input: {
            borderColor: "var(--mantine-color-primaryGrey-1)",
            backgroundColor: "var(--mantine-color-primaryGrey-1)",
          },
        }}
        {...props}
      />
    </MantineProvider>
  );
}

export default CustomCheckbox;
