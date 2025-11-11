import { SimpleGrid, SimpleGridProps } from "@mantine/core";

const CustomCards = ({
  children,
  ...props
}: { children: React.ReactNode } & SimpleGridProps) => {
  return (
    <SimpleGrid
      verticalSpacing={{ base: 0, sm: 30 }}
      spacing={{ base: 0, sm: 30 }}
      cols={{ base: 1, sm: 2, md: 2, lg: 3 }}
      {...props}
    >
      {children}
    </SimpleGrid>
  );
};
export default CustomCards;
