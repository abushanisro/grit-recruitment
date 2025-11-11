const badgeColors = [
  "primarySkyBlue.6",
  "secondarySkyBlue.4",
  "secondaryGreen.1",
];
export const getColor = (index: number): string => {
  const colorIndex = index % 3;
  return badgeColors[colorIndex];
};

export const getColorForPaymentStatus = (paymentStatus: string) => {
  if (paymentStatus == "success") {
    return "#67FB4F";
  } else if (paymentStatus == "pending") {
    return "#f0ca73";
  } else {
    return "#FF735B";
  }
};
