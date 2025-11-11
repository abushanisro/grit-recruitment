import {
  InputBaseProps,
  InputVariant,
  NumberInput,
  NumberInputProps,
  StylesApiProps,
  __InputStylesNames,
} from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import {
  Controller,
  FieldValues,
  Path,
  RegisterOptions,
  UseFormReturn,
} from "react-hook-form";
import get from "lodash/get";
import classes from "@/components/form/Form.module.css";

function CustomNumberInputField<T extends FieldValues>({
  hForm,
  name,
  rules,
  inputStyles,
  ...props
}: {
  hForm: UseFormReturn<T>;
  name: Path<T>;
  rules?: RegisterOptions;
  inputStyles?: StylesApiProps<{
    props: InputBaseProps;
    defaultRef: HTMLInputElement;
    defaultComponent: "input";
    stylesNames: __InputStylesNames;
    variant: InputVariant;
  }>;
} & NumberInputProps) {
  const {
    control,
    formState: { errors },
  } = hForm;
  const tablet = useMediaQuery("(max-width: 992px)");
  const error = get(errors, name);
  return (
    <Controller
      control={control}
      name={name}
      rules={rules}
      render={({
        field: { onChange, onBlur, value: fieldValue, name, ref },
      }) => (
        <NumberInput
          ref={ref}
          name={name}
          onChange={onChange}
          onBlur={onBlur}
          value={fieldValue}
          rightSectionProps={{ style: { zIndex: 0 } }}
          styles={{
            label: {
              color: "var(--mantine-color-secondaryGreen-1)",
              fontWeight: 700,
              fontSize: 17,
              paddingBottom: 10,
            },
            input: {
              color: "white",
              background: "transparent",
              borderRadius: 30,
              fontSize: tablet ? 16 : 20,
              paddingInline: 20,
              paddingBlock: 20,
            },
            error: {
              fontSize: tablet ? 12 : 14,
            },

            ...inputStyles?.styles,
          }}
          required={Boolean(rules?.required)}
          error={error && error.message ? String(error.message) : ""}
          className={classes.input}
          {...props}
        />
      )}
    />
  );
}

export default CustomNumberInputField;
