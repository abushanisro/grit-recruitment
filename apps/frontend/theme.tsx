import { createTheme, mergeThemeOverrides } from "@mantine/core";

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
    primarySkyBlue: [
      "#E1FEFF",
      "#D0F8FB",
      "#A7EDF3",
      "#79E4EA",
      "#55DBE3",
      "#3DD6DF",
      "#22D1DC", // Color from design
      "#21C8D4",
      "#12BBC6",
      "#00A7B0",
      "#009199",
    ],
    secondarySkyBlue: [
      "#E2F6FF",
      "#CBE9FF",
      "#99CFFF",
      "#62B5FF",
      "#40A3FF", // Color from design
      "#1890FF",
      "#0089FF",
      "#0076E5",
      "#0068CE",
      "#005AB7",
    ],
    primaryGreen: [
      "#e2fefc",
      "#d3f8f3",
      "#abede6",
      "#7AE2D6", // Color from design
      "#9be9e0",
      "#45d6c5",
      "#35d3c1",
      "#23baaa",
      "#0ba696",
      "#009082",
    ],
    secondaryGreen: [
      "#ecfdeb",
      "#CCF5C6", //color from design
      "#b2f0a8",
      "#8ce77f",
      "#6be059",
      "#55dc40",
      "#4ada33",
      "#3ac126",
      "#30ab1e",
      "#209512",
    ],
    primaryBlue: [
      "#ECF5FE",
      "#D7E7F8",
      "#A8CEF2",
      "#79B4EE",
      "#549DEB",
      "#4090EA",
      "#3589EA",
      "#2A76D1",
      "#095FAE",
      "#08559C", // Color from design
    ],

    secondaryBlue: [
      "#edf5fd",
      "#dae7f5",
      "#afceec",
      "#82b4e4",
      "#5f9dde",
      "#4a8fda",
      "#3e89da",
      "#3076c2",
      "#2669ad",
      "#061B2E", // Color from design
    ],
    primaryPaleBlue: [
      "#eff3fb",
      "#dce3f1",
      "#b3c3e3",
      "#89a2d7",
      "#6786cd",
      "#5174c7",
      "#456cc6",
      "#375baf",
      "#2f519c",
      "#124269 ", // Color from design
    ],

    primaryDarkBlue: [
      "#edf5fd",
      "#dae7f5",
      "#afceec",
      "#82b4e4",
      "#5f9dde",
      "#4a8fda",
      "#3e89da",
      "#3076c2",
      "#2669ad",
      "#061B2E", //Color from design
    ],
    secondaryDarkBlue: [
      "#f0f2fa",
      "#dee1ee",
      "#b8bfde",
      "#909ccf",
      "#6e7ec2",
      "#5a6bbb",
      "#4e61b8",
      "#4051a2",
      "#374892",
      "#1B264F", //Color from design
    ],

    primaryGrey: [
      "#ecfdeb33",
      "#ccf5c633", //Color from design
      "#b5f0ac33",
      "#8ce77f33",
      "#6be05933",
      "#55dc4033",
      "#4ada3333",
      "#3ac12633",
      "#30ab1e33",
      "#20951233",
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

export const myTheme = mergeThemeOverrides(
  breakpointsTheme,
  fontFamily,
  colorsTheme
);
