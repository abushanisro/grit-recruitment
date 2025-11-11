import {
  Controller,
  FieldValues,
  Path,
  RegisterOptions,
  UseFormReturn,
} from "react-hook-form";
import {
  __InputStylesNames,
  Radio,
  Group,
  Text,
  createTheme,
  MantineProvider,
} from "@mantine/core";
import { CustomSelectOption } from "@/types/form";
import get from "lodash/get";

const theme = createTheme({
  cursorType: "pointer",
});

function CustomRadioBox<T extends FieldValues>({
  hForm,
  name,
  rules,
  options,
}: {
  hForm: UseFormReturn<T>;
  name: Path<T>;
  options: CustomSelectOption[];
  rules?: RegisterOptions;
}) {
  const {
    control,
    formState: { errors },
  } = hForm;
  const error = get(errors, name);
  return (
    <Controller
      control={control}
      name={name}
      rules={rules}
      render={({ field: { onChange, value: fieldValue, name, ref } }) => (
        <MantineProvider theme={theme}>
          <Radio.Group
            ref={ref}
            name={name}
            onChange={onChange}
            value={fieldValue}
            required={Boolean(rules?.required)}
            error={
              error && error.message ? (
                <Text py={10}>{`${error.message}`}</Text>
              ) : (
                ""
              )
            }
          >
            <Group>
              {options.map((option, index) => {
                return (
                  <Radio
                    key={index}
                    color="var(--mantine-color-secondaryGreen-3)"
                    styles={{
                      label: {
                        color: "white",
                      },
                    }}
                    label={option.label}
                    value={option.value}
                  />
                );
              })}
            </Group>
          </Radio.Group>
        </MantineProvider>
      )}
    />
  );
}

export default CustomRadioBox;
