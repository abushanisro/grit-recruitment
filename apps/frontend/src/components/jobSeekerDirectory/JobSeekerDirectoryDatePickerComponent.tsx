import { DatePickerInput } from "@mantine/dates";
import { UseFormReturn } from "react-hook-form";
import { JobseekerDirectoryFilterInterface } from "@/types/jobSeeker";
import isNil from "lodash/isNil";
import dayjs from "dayjs";
import classes from "styles/editButton.module.css";

const JobSeekerDirectoryDatePickerComponent = ({
  hForm,
}: {
  hForm: UseFormReturn<JobseekerDirectoryFilterInterface>;
}) => {
  const { setValue, watch, register, formState } = hForm;
  const { errors } = formState;
  const currentDate = dayjs();
  const dateTenDaysAgo = currentDate.subtract(10, "day");
  const formattedDate = dateTenDaysAgo.format("YYYY-MM-DD");
  const date = new Date(formattedDate);
  const isDateValid = watch("lastLogin");
  const isDateRequired = !isNil(isDateValid) && isDateValid.value === "custom";
  if (!isDateValid || isDateValid.value !== "custom") {
    return <></>;
  }

  return (
    <DatePickerInput
      {...register("customDate", {
        required: isDateRequired ? "Please select a date" : false,
      })}
      placeholder="Please select a date"
      type="range"
      valueFormat="DD/MM/YYYY"
      onChange={(value) => setValue("customDate", value)}
      className={classes.datePickerPlaceholder}
      error={
        errors && errors.customDate ? String(errors.customDate.message) : ""
      }
      defaultValue={[date, new Date()]}
      maxDate={new Date()}
      styles={{
        day: { color: "black" },
        input: {
          borderRadius: 30,
          background: "linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)",
          fontWeight: 600,
          paddingLeft: 20,
        },
      }}
    />
  );
};

export default JobSeekerDirectoryDatePickerComponent;
