import React, { useState } from "react";
import orderBy from "lodash/orderBy";
import { Table, Box, Flex } from "@mantine/core";

import { SortableTableProps, SortHeader } from "@/types/sortableTable";
import { useMediaQuery } from "@mantine/hooks";
import classes from "styles/editButton.module.css";
import { IconCaretUpFilled } from "@tabler/icons-react";

const isString = (value: any) => {
  return typeof value === "string" || value instanceof String;
};

const convertToLowerCase = (val: any) => {
  if (isString(val)) return val.toLowerCase();
  return val;
};

function sortData<T>(
  headers: SortHeader<T>[],
  data: T[],
  index: number,
  isAsc: boolean
) {
  if (index === null) {
    return data;
  }

  const valueFunc = headers[index].valueFunc;

  return orderBy(
    data,
    [
      (item) => {
        return convertToLowerCase(valueFunc(item));
      },
    ],
    [isAsc ? "asc" : "desc"]
  );
}

function SortableTable<T>(props: SortableTableProps<T>): JSX.Element {
  const { data } = props;

  const [activeHeaderIndex, setActiveHeaderIndex] = useState(
    props.initialSortData.index
  );
  const [isAsc, setIsAsc] = useState(props.initialSortData.isAsc);

  const handleHeaderClick = (index: number) => {
    if (index === activeHeaderIndex) {
      setIsAsc(!isAsc);
    } else {
      setIsAsc(false);
    }
    setActiveHeaderIndex(index);
  };

  const sortedData = sortData<T>(props.headers, data, activeHeaderIndex, isAsc);
  const mobile = useMediaQuery("(max-width:426px)");

  return (
    <Box maw="100%">
      <Table
        borderColor="secondaryGreen.1"
        style={{
          size: props.tableSize || "md",
        }}
        verticalSpacing={mobile ? "xs" : "md"}
        horizontalSpacing="lg"
      >
        <Table.Thead
          pos="sticky"
          top="-1px"
          style={{
            zIndex: 1,
          }}
        >
          <Table.Tr bg={"secondaryBlue.9"}>
            {props.headers.map((header, index) => {
              return (
                <Table.Th
                  lh="1.75"
                  fz={{ base: 16, md: 20 }}
                  className={classes.tableHeader}
                  fw={600}
                  c="primarySkyBlue.6"
                  style={{
                    cursor: header.disableSort ? "auto" : "pointer",

                    whiteSpace: "nowrap",
                  }}
                  miw="160px"
                  key={`th-${index}`}
                  onClick={() => {
                    if (header.disableSort) {
                      return;
                    }
                    handleHeaderClick(index);
                  }}
                >
                  <Flex gap="2" wrap={"nowrap"}>
                    {header.name}
                    {!header.disableSort && (
                      <IconCaretUpFilled
                        color={
                          index === activeHeaderIndex ? "#706e6b" : "#cccfd1"
                        }
                        style={{
                          transform:
                            isAsc && index === activeHeaderIndex
                              ? `rotate(180deg)`
                              : `rotate(0deg)`,
                          marginTop: mobile ? 2 : 8,
                        }}
                        size={18}
                      />
                    )}
                  </Flex>
                </Table.Th>
              );
            })}
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody fz={{ base: 14, md: 16 }}>
          {props.body(sortedData)}
        </Table.Tbody>
      </Table>
    </Box>
  );
}

export default SortableTable;
