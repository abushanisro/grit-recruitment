import { Pagination, PaginationProps } from "@mantine/core";
import classes from "@/components/CustomPagination.module.css";

const CustomPagination = ({ ...props }: PaginationProps) => {
  return (
    <Pagination
      color="primarySkyBlue.6"
      style={{ zIndex: 10 }}
      classNames={{ control: classes.customControl }}
      {...props}
    />
  );
};

export default CustomPagination;
