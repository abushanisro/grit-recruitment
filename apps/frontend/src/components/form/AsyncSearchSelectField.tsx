import { Text, TextProps, Input } from "@mantine/core";
import get from "lodash/get";
import {
  Controller,
  FieldValues,
  Path,
  RegisterOptions,
  UseFormReturn,
} from "react-hook-form";
import { CustomSelectOption } from "@/types/form";
import AsyncSearchSelect from "@/components/form/AsyncSearchSelect";
import { GroupBase, StylesConfig } from "react-select";

export interface AsyncSearchSelectFieldProps<
  T extends FieldValues = FieldValues
> {
  hForm: UseFormReturn<T>;
  name: Path<T>;
  label?: string;
  isDisabled?: boolean;
  rules?: RegisterOptions;
  placeholder?: string;
  getOptions: (v: string) => Promise<CustomSelectOption[]>;
  instanceId: string;
  showDropdownOnModal?: boolean;
  dropDownIcon?: React.ReactNode;
  customStyles?: StylesConfig<
    CustomSelectOption,
    boolean,
    GroupBase<CustomSelectOption>
  >;
  labelProps?: TextProps;
  isMulti?: boolean;
  showAsterisk?: boolean;
  controlShouldRenderValue?: boolean;
  backspaceRemovesValue?: boolean;
  isClearable?: boolean;
  isSearchable?: boolean;
  showLoading?: boolean;
}

function AsyncSearchSelectField<T extends FieldValues>({
  hForm,
  name,
  label,
  isDisabled,
  rules,
  placeholder,
  getOptions,
  instanceId,
  showDropdownOnModal,
  dropDownIcon,
  customStyles,
  labelProps,
  isMulti,
  showAsterisk = true,
  controlShouldRenderValue = true,
  backspaceRemovesValue = true,
  isClearable,
  isSearchable,
  showLoading = true,
}: AsyncSearchSelectFieldProps<T>) {
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
      render={({
        field: { onChange, onBlur, value: fieldValue, name, ref },
      }) => (
        <>
          {label && (
            <Text {...labelProps}>
              {label}{" "}
              {rules?.required && showAsterisk ? (
                <Text span c="red">
                  *
                </Text>
              ) : (
                ""
              )}
            </Text>
          )}
          <AsyncSearchSelect
            ref={ref}
            instanceId={instanceId}
            name={name}
            onChange={onChange}
            onMultipleChange={isMulti ? onChange : undefined}
            onBlur={onBlur}
            value={fieldValue}
            placeholder={placeholder ?? "Select a value"}
            isDisabled={isDisabled}
            isMulti={isMulti}
            getOptions={getOptions}
            showDropdownOnModal={showDropdownOnModal}
            dropDownIcon={dropDownIcon}
            customStyles={customStyles}
            isClearable={isClearable}
            isSearchable={isSearchable}
            controlShouldRenderValue={controlShouldRenderValue}
            backspaceRemovesValue={backspaceRemovesValue}
            showLoading={showLoading}
          />
          {Boolean(error) && (
            <Input.Error fz={{ base: 12, sm: 14 }} mt={4}>
              <>{error?.message ?? "Field error"}</>
            </Input.Error>
          )}
        </>
      )}
    />
  );
}

export default AsyncSearchSelectField;
