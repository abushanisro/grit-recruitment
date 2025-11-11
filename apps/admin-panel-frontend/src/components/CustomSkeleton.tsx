import {
  MantineThemeProvider,
  Skeleton,
  SkeletonProps,
  createTheme,
} from "@mantine/core";

import classes from "styles/skeleton.module.css";

const theme = createTheme({
  components: {
    Skeleton: Skeleton.extend({
      classNames: {
        root: classes.customSkeleton,
      },
    }),
  },
});

const CustomSkeleton = ({ ...props }: SkeletonProps) => {
  return (
    <MantineThemeProvider theme={theme}>
      <Skeleton {...props} />
    </MantineThemeProvider>
  );
};

export default CustomSkeleton;
