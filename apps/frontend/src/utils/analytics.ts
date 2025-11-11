export const getOriginUrl = () => {
  if (typeof window !== "undefined") {
    return window.location.href;
  }
  return "";
};
