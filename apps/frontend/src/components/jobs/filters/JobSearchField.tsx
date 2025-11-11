import { Image, Input, CloseButton, InputProps } from "@mantine/core";
import {
  Controller,
  FieldValues,
  Path,
  RegisterOptions,
  UseFormReturn,
} from "react-hook-form";

interface JobSearchFieldProps<T extends FieldValues> {
  rules?: RegisterOptions;
  name: Path<T>;
  hForm: UseFormReturn<T>;
  onReset?: () => void;
  inputStyle?: Record<string, any>;
  placeholder?: string;
}

function JobSearchField<T extends FieldValues>({
  rules,
  name,
  hForm,
  placeholder,
  inputStyle,
  onReset,
  ...props
}: JobSearchFieldProps<T> & Omit<InputProps, "name">) {
  const { control } = hForm;
  return (
    <Controller
      control={control}
      name={name}
      rules={rules}
      render={({
        field: { onChange, onBlur, value: fieldValue, name, ref },
      }) => (
        <Input
          ref={ref}
          name={name}
          onChange={onChange}
          onBlur={onBlur}
          value={fieldValue}
          maw={463}
          styles={{
            input: {
              border: "0.6px solid",
              borderColor: "white",
              borderRadius: 19,
              color: "white",
              backgroundColor: "transparent",

              ...inputStyle,
            },
            section: {
              paddingLeft: 16,
              paddingRight: 12,
            },
          }}
          placeholder={placeholder ?? "Search"}
          rightSectionPointerEvents="all"
          mt="md"
          leftSection={
            <Image src="/images/search.svg" alt="search icon" w={16} h={16} />
          }
          rightSection={
            <CloseButton
              aria-label="Clear input"
              onClick={onReset}
              style={{
                display: fieldValue ? undefined : "none",
                borderRadius: "100%",
              }}
            />
          }
          {...props}
        />
      )}
    />
  );
}
export default JobSearchField;
