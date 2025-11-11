import {
  SelectProps,
  Select,
  __InputStylesNames,
  StylesApiProps,
  InputVariant,
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
import { CustomSelectOption } from "@/types/form";

function CustomSelect<T extends FieldValues>({
  hForm,
  name,
  options,
  rules,
  inputStyles,
  ...props
}: {
  hForm: UseFormReturn<T>;
  name: Path<T>;
  options: CustomSelectOption[];
  rules?: RegisterOptions;
  inputStyles?: StylesApiProps<{
    props: SelectProps;
    defaultRef: HTMLInputElement;
    defaultComponent: "input";
    stylesNames: __InputStylesNames;
    variant: InputVariant;
  }>;
} & SelectProps) {
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
        <Select
          ref={ref}
          data={options}
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
            dropdown: {
              background: "black",
            },
            option: {
              fontSize: tablet ? 14 : 16,
            },
            options: {
              color: "white",
              background: "black",
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

export default CustomSelect;
