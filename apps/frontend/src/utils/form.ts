import { CustomSelectOption } from "@/types/form";

export const checkIsValidUrl = (value: string) => {
  try {
    if (!value) {
      return true;
    }
    new URL(value);
    return true; // Valid URL
  } catch (error) {
    return "Enter a valid link e.g: https://www.website.com"; // Invalid URL
  }
};

export const convertOptionsToNumberList = (options: CustomSelectOption[]) =>
  options?.map((eachOption) => Number(eachOption.value));
