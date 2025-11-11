import { Box, Flex } from "@mantine/core";
import AsyncSearchSelectField from "@components/form/AsyncSearchSelectField";
import {
  FieldValues,
  Path,
  RegisterOptions,
  UseFormReturn,
} from "react-hook-form";
import { useMediaQuery } from "@mantine/hooks";
import SelectedOptionWithRemoveButtonBadge from "./SelectedOptionWithRemoveButtonBadge";
import { CustomSelectOption } from "@/types/form";
import { IconChevronDown } from "@tabler/icons-react";
import { Fragment } from "react";
import InlineLabelWithInputField from "./InlineLabelWithInputField";

const selectFieldRenderOptionsOutsideStyles = (tablet: boolean | undefined) => {
  return {
    control: () => ({
      fontSize: tablet ? 14 : 16,
      width: tablet ? "100%" : 170,
      backgroundColor: "transparent",
      border: "1px solid white",
      color: "white",
    }),
    input: () => ({ color: "white" }),
    placeholder: () => {
      return { color: "white", fontWeight: 400 };
    },
    option: () => {
      return { color: "white" };
    },
    menu: () => {
      return { maxWidth: tablet ? "100%" : 200 };
    },
    menuList: () => {
      return { maxWidth: tablet ? "100%" : 200 };
    },
    multiValueLabel: () => {
      return { fontSize: 14 };
    },
    singleValue: () => {
      return { color: "white", fontWeight: 600 };
    },
  };
};
function AsyncSelectFieldWithOptionsOutside<T extends FieldValues>({
  hForm,
  name,
  getOptions,
  label,
  rules,
  placeholder = "Type here...",
}: {
  hForm: UseFormReturn<T>;
  name: Path<T>;
  getOptions: (v: string) => Promise<CustomSelectOption[]>;
  label: string;
  rules?: RegisterOptions;
  placeholder?: string;
}) {
  const tablet = useMediaQuery("(max-width: 992px)");
  const { watch, setValue } = hForm;
  const selectedValues = watch(name);
  return (
    <InlineLabelWithInputField
      label={label}
      isRequired={Boolean(rules?.required)}
    >
      <Flex wrap="wrap" direction={"row"} gap={10}>
        <Box w={{ base: "100%", sm: "max-content" }}>
          <AsyncSearchSelectField
            hForm={hForm}
            name={name}
            rules={rules}
            isMulti={true}
            placeholder={placeholder}
            controlShouldRenderValue={false}
            backspaceRemovesValue={false}
            getOptions={getOptions}
            instanceId={name}
            customStyles={selectFieldRenderOptionsOutsideStyles(tablet)}
            dropDownIcon={<IconChevronDown size={20} aria-label="down" />}
          />
        </Box>
        {selectedValues?.map((eachLocationOption: T, index: number) => {
          return (
            <Fragment key={index}>
              <SelectedOptionWithRemoveButtonBadge
                onClick={() => {
                  setValue(
                    name,
                    selectedValues.filter(
                      (eachValue: T) =>
                        eachValue.value !== eachLocationOption.value
                    )
                  );
                }}
                h="100%"
                mah={40}
                label={eachLocationOption.label}
              />
            </Fragment>
          );
        })}
      </Flex>
    </InlineLabelWithInputField>
  );
}
export default AsyncSelectFieldWithOptionsOutside;
