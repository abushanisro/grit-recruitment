export const getSelectFieldStyles = (tablet: boolean | undefined) => {
  return {
    control: () => ({
      maxWidth: "100%",
      fontSize: tablet ? 14 : 16,
      backgroundColor: "transparent",
      border: "1px solid white",
      color: "white",
    }),
    input: () => ({ color: "white" }),
    placeholder: () => {
      return { color: "var(--mantine-color-customGray-4)", fontWeight: 400 };
    },
    
    option: () => {
      return { color: "white" };
    },
    singleValue: () => {
      return { color: "white" };
    },
    
  };
};
