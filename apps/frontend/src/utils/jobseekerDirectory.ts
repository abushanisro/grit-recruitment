export const getAppliedFilterCount = (
  watchFormValues: any,
  defaultValueObj: any
) => {
  return Object.keys(watchFormValues).filter((key) => {
    const value = watchFormValues[key];
    const isNotEmptyObject =
      typeof value === "object" &&
      value !== null &&
      Object.keys(value).length > 0;
    return (
      key !== "pageNumber" &&
      key !== "activeSubscription" &&
      JSON.stringify(value) !== JSON.stringify(defaultValueObj[key]) &&
      isNotEmptyObject
    );
  }).length;
};
