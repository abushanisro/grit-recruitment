import {
  InputBaseProps,
  InputVariant,
  StylesApiProps,
  Textarea,
  TextareaProps,
  __InputStylesNames,
} from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import {
  FieldValues,
  Path,
  RegisterOptions,
  UseFormReturn,
} from "react-hook-form";
import get from "lodash/get";
import classes from "@/components/form/Form.module.css";

function CustomTextArea<T extends FieldValues>({
  hForm,
  name,
  rules,
  inputStyles,
  ...props
}: {
  hForm: UseFormReturn<T, any, undefined>;
  name: Path<T>;
  rules?: RegisterOptions;
  inputStyles?: StylesApiProps<{
    props: InputBaseProps;
    defaultRef: HTMLInputElement;
    defaultComponent: "input";
    stylesNames: __InputStylesNames;
    variant: InputVariant;
  }>;
} & TextareaProps) {
  const {
    register,
    formState: { errors },
  } = hForm;
  const tablet = useMediaQuery("(max-width: 992px)");
  const error = get(errors, name);
  return (
    <Textarea
      {...register(name, rules)}
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
          borderRadius: 15,
          fontSize: tablet ? 16 : 20,
          paddingInline: 20,
          paddingBlock: 20,
          height:120 
        },
        ...inputStyles?.styles,
      }}
      required={Boolean(rules?.required)}
      error={error && error.message ? String(error.message) : ""}
      className={classes.input}
      {...props}
    />
  );
}

export default CustomTextArea;
