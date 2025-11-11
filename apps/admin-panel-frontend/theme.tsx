import {
  MantineThemeOverride,
  createTheme,
  mergeThemeOverrides,
} from "@mantine/core";

const breakpointsTheme = createTheme({
  breakpoints: {
    base: "320px",
    xs: "480px",
    sm: "768px",
    md: "1024px",
    lg: "1184px",
    xl: "1440px",
  },
});

const fontFamily = createTheme({
  fontFamily: "Work Sans,Gaegu, Open Sans, sans-serif",
});
const colorsTheme = createTheme({
  colors: {
    secondaryYellow: [
      "#fff8e3",
      "#fbefd0",
      "#f5dda4",
      "#f0ca73", //Color from design
      "#ecba4b",
      "#e9af30",
      "#e7aa21",
      "#cd9513",
      "#b78408",
      "#9f7100",
    ],
    primaryOrange: [
      "#fff1e1",
      "#ffe2cb",
      "#ffc39a",
      "#ffa365",
      "#fd8837",
      "#fd761a", // Color from design
      "#fe6c08",
      "#e25b00",
      "#ca5000",
      "#b04300",
    ],
    secondaryOrange: [
      "#ffebe6",
      "#ffd6ce",
      "#ffaa9b",
      "#FF735B", //Color from design
      "#fe5437",
      "#fe3b19",
      "#ff2d09",
      "#e41f00",
      "#cb1700",
      "#b10900",
    ],
    borderOrange: [
      "#ffebe6",
      "#ffd6ce",
      "#ffaa9b",
      "#ff7b64", //Color from design
      "#fe5437",
      "#fe3b19",
      "#ff2d09",
      "#e41f00",
      "#cb1700",
      "#b10900",
    ],
    secondaryRed: [
      "#ffe8eb",
      "#ffcfd4",
      "#fe9ca7",
      "#fc6977", //Color from design
      "#fa3b4d",
      "#f91f33",
      "#fa0e24",
      "#e00018",
      "#c80014",
      "#261012", //Color from design
    ],
    customGray: [
      "#f5f5f5",
      "#e7e7e7",
      "#D9D9D9", //Color from design
      "#cdcdcd", //Color from design
      "#9E9E9E", //Color from design
      "#8b8b8b",
      "#848484",
      "#717171",
      "#656565",
      "#575757",
    ],
    customBlack: [
      "#262626", //Color from design
      "#262626",
      "#262626",
      "#121212",
      "#121212",
      "#121212", //Color from design
      "#121212",
      "#000000",
      "#000000", //Color from design
      "#000000",
    ],
  },
});

export const myTheme: MantineThemeOverride = mergeThemeOverrides(
  breakpointsTheme,
  fontFamily,
  colorsTheme
);
