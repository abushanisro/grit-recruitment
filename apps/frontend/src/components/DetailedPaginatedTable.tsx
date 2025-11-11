import { PaginationInterface } from "@/types/employer";
import { SortHeader } from "@/types/sortableTable";
import { Box } from "@mantine/core";
import React from "react";
import SortableTable from "@/components/SortableTable";
import { Center } from "@mantine/core";
import CustomPagination from "./CustomPagination";
import { useMediaQuery } from "@mantine/hooks";

function DetailedPaginatedTable<T>({
  tableData,
  tableBody,
  tableHeaderSection,
  totalPages,
  pageNumber,
  setPageNumber,
  normalPagination,
  hasSorting = true,
  checkbox,
  ...props
}: {
  tableData: PaginationInterface<T>;
  tableBody: (data: T[]) => React.ReactNode;
  tableHeaderSection: SortHeader<T>[];
  pageNumber: number;
  totalPages: number;
  setPageNumber: (value: number) => void;

  normalPagination?: boolean;
  hasSorting?: boolean;

  checkbox?: React.ReactNode;
}) {
  const isMobile = useMediaQuery("(max-width: 426px)");

  return (
    <Box mb={2}>
      <Box
        w="100%"
        style={{
          overflowX: "auto",
          border: "1px solid",
          borderColor: "var(--mantine-color-secondaryGreen-1)",
          boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)",
          borderRadius: isMobile ? 8 : 20,
        }}
        {...props}
      >
        <SortableTable
          headers={tableHeaderSection}
          data={tableData.results}
          initialSortData={{ index: 0, isAsc: false }}
          body={tableBody}
          tableSize="md"
        />
      </Box>
      <Center w="100%" p="10" bg="transparent" mx={0} mt={30}>
        <CustomPagination
          bg="transparent"
          size={isMobile ? "sm" : "md"}
          value={pageNumber}
          onChange={(e) => {
            setPageNumber(e);
          }}
          total={totalPages}
        />
      </Center>
    </Box>
  );
}
export default DetailedPaginatedTable;
