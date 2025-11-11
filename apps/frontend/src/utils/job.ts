export const formatPrice = (price: number | string) => {
  return `₹ ${price.toLocaleString("en-IN")}`;
};
