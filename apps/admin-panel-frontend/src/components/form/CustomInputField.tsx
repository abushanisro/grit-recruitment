import {
  InputBaseProps,
  InputVariant,
  StylesApiProps,
  TextInput,
  TextInputProps,
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

function CustomInputField<T extends FieldValues>({
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
} & TextInputProps) {
  const {
    formState: { errors },
    control,
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
        <TextInput
          ref={ref}
          name={name}
          onChange={onChange}
          onBlur={onBlur}
          value={fieldValue}
          styles={{
            label: {
              color: "var(--mantine-color-secondaryYellow-3)",
              fontWeight: 700,
              fontSize: 17,
              paddingBottom: 10,
            },
            input: {
              color: "white",
              background: "black",
              borderRadius: 30,
              fontSize: tablet ? 16 : 20,
              paddingInline: 20,
              paddingBlock: 20,
            },
            section: { zIndex: 10 },
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

export default CustomInputField;
