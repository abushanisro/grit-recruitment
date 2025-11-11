export interface SortHeader<T> {
  name: React.ReactNode;
  valueFunc: (v: T) => any;
  disableSort?: boolean;
}

export interface SortableTableInitialSortData {
  index: number;
  isAsc: boolean;
}

export interface SortableTableProps<T> {
  variant?: string;
  headers: SortHeader<T>[];
  data: T[];
  body: (data: T[]) => React.ReactNode;
  emptyMsg?: string;
  initialSortData: SortableTableInitialSortData;
  tableSize?: string;
}
