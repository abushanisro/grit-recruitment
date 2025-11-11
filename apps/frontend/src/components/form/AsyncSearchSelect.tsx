import {
  components,
  CSSObjectWithLabel,
  GroupBase,
  MultiValue,
  PropsValue,
  SingleValue,
  StylesConfig,
  ControlProps,
  PlaceholderProps,
  IndicatorSeparatorProps,
  MultiValueProps,
  MenuListProps,
  OptionProps,
  NoticeProps,
  SingleValueProps,
  InputProps,
} from "react-select";
import AsyncSelect from "react-select/async";
import { CustomSelectOption } from "@/types/form";
import { IconX } from "@tabler/icons-react";
import { Noop } from "react-hook-form";
import {
  MenuProps,
  PortalStyleArgs,
} from "react-select/dist/declarations/src/components/Menu";
import { Ref, forwardRef } from "react";
import Select from "react-select/dist/declarations/src/Select";

const getStyles = (
  customStyles:
    | StylesConfig<CustomSelectOption, boolean, GroupBase<CustomSelectOption>>
    | undefined
) => {
  return {
    control: (
      baseStyles: CSSObjectWithLabel,
      props: ControlProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => {
      return {
        ...baseStyles,
        fontSize: "16px",
        fontWeight: "600",
        minHeight: "40px",
        borderRadius: "20px",
        borderColor: "black",
        paddingInline: "10px",
        backgroundColor: "var(--mantine-color-secondaryOrange-3)",
        ...customStyles?.control?.(baseStyles, props),
      };
    },
    input: (
      baseStyles: CSSObjectWithLabel,
      props: InputProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => ({
      ...baseStyles,
      ...customStyles?.input?.(baseStyles, props),
    }),
    placeholder: (
      baseStyles: CSSObjectWithLabel,
      props: PlaceholderProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => ({
      ...baseStyles,
      color: "black",
      ...customStyles?.placeholder?.(baseStyles, props),
    }),
    menuPortal: (baseStyles: CSSObjectWithLabel, props: PortalStyleArgs) => {
      return {
        ...baseStyles,
        fontSize: "16px",
        backgroundColor: "black",
        color: "white",
        zIndex: 9999,
        ...customStyles?.menuPortal?.(baseStyles, props),
      };
    },
    indicatorSeparator: (
      baseStyles: CSSObjectWithLabel,
      props: IndicatorSeparatorProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => ({
      display: "none",
      ...customStyles?.indicatorSeparator?.(baseStyles, props),
    }),
    option: (
      baseStyles: CSSObjectWithLabel,
      props: OptionProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => {
      return {
        ...baseStyles,
        ":hover": {
          ...baseStyles[":active"],
          backgroundColor: "var(--mantine-color-primaryGrey-1)",
          cursor: "pointer",
        },
        padding: "10px",
        backgroundColor: "black",
        color: "white",
        fontSize: "16px",
        cursor: "pointer",
        textTransform: "capitalize",
        ":active": {
          ...baseStyles[":active"],
          backgroundColor: !props.isDisabled
            ? props.isSelected
              ? "var(--mantine-color-customBlack-1)"
              : "var(--mantine-color-customBlack-1)"
            : undefined,
        },
        ...customStyles?.option?.(baseStyles, props),
      } as CSSObjectWithLabel; // TODO : Fix type error as textTransform: "capitalize", is not currently accepted
    },
    noOptionsMessage: (
      baseStyles: CSSObjectWithLabel,
      props: NoticeProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => ({
      padding: "10px",
      backgroundColor: "black",
      color: "white",
      ...customStyles?.noOptionsMessage?.(baseStyles, props),
    }),
    menu: (
      baseStyles: CSSObjectWithLabel,
      props: MenuProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => ({
      ...baseStyles,
      zIndex: 2,
      ...customStyles?.menu?.(baseStyles, props),
    }),
    menuList: (
      baseStyles: CSSObjectWithLabel,
      props: MenuListProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => ({
      backgroundColor: "black",
      borderColor: "white",
      zIndex: 1000,
      maxHeight: "200px !important",
      overflow: "auto",
      ...customStyles?.menuList?.(baseStyles, props),
    }),
    multiValue: (
      baseStyles: CSSObjectWithLabel,
      props: MultiValueProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => ({
      ...baseStyles,
      backgroundColor: "black",
      paddingInline: "4px",
      borderRadius: "10px",
      borderColor: "white",
      fontWeight: 600,
      ...customStyles?.multiValue?.(baseStyles, props),
    }),
    singleValue: (
      baseStyles: CSSObjectWithLabel,
      props: SingleValueProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => {
      return {
        ...baseStyles,
        textTransform: "capitalize",
        color: "black",
        fontWeight: 600,
        ...customStyles?.singleValue?.(baseStyles, props),
      } as CSSObjectWithLabel; // TODO : Fix type error as textTransform: "capitalize", is not currently accepted),
    },
    multiValueLabel: (
      baseStyles: CSSObjectWithLabel,
      props: MultiValueProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => {
      return {
        ...baseStyles,
        color: "white",
        paddingRight: "16px",
        textTransform: "capitalize",
        ...customStyles?.multiValueLabel?.(baseStyles, props),
      } as CSSObjectWithLabel; // TODO : Fix type error as textTransform: "capitalize", is not currently accepted),
    },
    multiValueRemove: (
      baseStyles: CSSObjectWithLabel,
      props: MultiValueProps<
        CustomSelectOption,
        boolean,
        GroupBase<CustomSelectOption>
      >
    ) => {
      return {
        ...baseStyles,
        ":hover": {
          ...baseStyles[":active"],
          backgroundColor: "black",
          borderRadius: "20px",
          cursor: "pointer",
        },
        color: "white",
        ...customStyles?.multiValueRemove?.(baseStyles, props),
      };
    },
  };
};

const AsyncSearchSelect = forwardRef(
  (
    {
      name,
      isDisabled,
      value,
      onChange,
      onMultipleChange,
      placeholder,
      getOptions,
      instanceId,
      showDropdownOnModal,
      dropDownIcon,
      customStyles,
      isMulti,
      onBlur,
      controlShouldRenderValue = true,
      backspaceRemovesValue = true,
      isClearable = true,
      isSearchable = true,
      showLoading = true,
    }: {
      name?: string;
      isDisabled?: boolean;
      value: PropsValue<CustomSelectOption>;
      onChange: (value: CustomSelectOption | null) => void;
      onMultipleChange?: (value: CustomSelectOption[]) => void;
      onBlur?: Noop;
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
      isMulti?: boolean;
      controlShouldRenderValue?: boolean;
      backspaceRemovesValue?: boolean;
      isClearable?: boolean;
      isSearchable?: boolean;
      showLoading?: boolean;
    },
    ref: Ref<
      Select<CustomSelectOption, boolean, GroupBase<CustomSelectOption>>
    > | null
  ) => {
    const DropDownIndicator = (props: any) => {
      if (dropDownIcon === undefined) {
        return null;
      }
      return (
        <components.DropdownIndicator {...props}>
          {dropDownIcon}
        </components.DropdownIndicator>
      );
    };

    const ClearIndicator = (props: any) => {
      if (isMulti) {
        return null;
      }
      return (
        <components.ClearIndicator {...props}>
          <IconX size="14px" color="var(--mantine-color-customGray-9)" />
        </components.ClearIndicator>
      );
    };

    const onSelect = (
      value:
        | MultiValue<CustomSelectOption>
        | SingleValue<CustomSelectOption>
        | null
    ) => {
      if (value === null) {
        onChange(null);
        return;
      }
      if (isMulti && onMultipleChange) {
        onMultipleChange([...(value as MultiValue<CustomSelectOption>)]);
        return;
      }
      onChange(value as SingleValue<CustomSelectOption>);

      return;
    };

    return (
      <AsyncSelect
        ref={ref}
        aria-label={`searchField-${name}`}
        instanceId={instanceId}
        name={name}
        onChange={onSelect}
        cacheOptions
        defaultOptions
        isClearable={isClearable}
        isSearchable={isSearchable}
        loadOptions={getOptions}
        onBlur={onBlur}
        value={value}
        placeholder={placeholder ?? "Select a value"}
        isDisabled={isDisabled}
        styles={getStyles(customStyles)}
        isMulti={isMulti}
        components={
          showLoading
            ? {
                DropdownIndicator: DropDownIndicator,
                ClearIndicator: ClearIndicator,
              }
            : {
                DropdownIndicator: DropDownIndicator,
                ClearIndicator: ClearIndicator,
                LoadingIndicator: () => null,
              }
        }
        menuPortalTarget={showDropdownOnModal === true ? document.body : null}
        controlShouldRenderValue={controlShouldRenderValue}
        backspaceRemovesValue={backspaceRemovesValue}
      />
    );
  }
);
AsyncSearchSelect.displayName = "AsyncSearchSelect";
export default AsyncSearchSelect;
