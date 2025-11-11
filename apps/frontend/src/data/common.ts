import { CommonInfoType } from "@/types/jobs";

const getFilteredLocationOptionsData = ({
  data,
}: {
  data: CommonInfoType[];
}) => {
  const filteredLocationOptions = data.filter(
    (eachValue) => !["Remote", "Anywhere in India"].includes(eachValue.name)
  );
  const locationOptionsData = filteredLocationOptions.map((eachValue) => ({
    label: eachValue.name,
    value: eachValue.id.toString(),
  }));
  return locationOptionsData;
};

export const getLocationOptionsData = ({
  data,
}: {
  data: CommonInfoType[];
}) => {
  const locationOptionsData = data.map((eachValue) => ({
    label: eachValue.name,
    value: eachValue.id.toString(),
  }));
  return locationOptionsData;
};

export default getFilteredLocationOptionsData;
