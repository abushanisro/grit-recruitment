import {
  MantineThemeProvider,
  Skeleton,
  SkeletonProps,
  Stack,
  createTheme,
} from "@mantine/core";

import classes from "styles/skeleton.module.css";
import CustomSkeleton from "@/components/CustomSkeleton";

const lightTheme = createTheme({
  components: {
    Skeleton: Skeleton.extend({
      classNames: {
        root: classes.lightCustomSkeleton,
      },
    }),
  },
});

const LightCustomSkeleton = ({ ...props }: SkeletonProps) => {
  return (
    <MantineThemeProvider theme={lightTheme}>
      <Skeleton {...props} />
    </MantineThemeProvider>
  );
};
const JobListSkeletonCard = () => {
  return (
    <>
      <CustomSkeleton h={200} style={{ borderRadius: 38 }}>
        <Stack
          justify="flex-start"
          align="flex-start"
          h="100%"
          px={50}
          py={45}
          gap={10}
        >
          <LightCustomSkeleton
            h={10}
            maw="60%"
            style={{ borderRadius: 6, zIndex: 10 }}
          />
          <LightCustomSkeleton
            maw="80%"
            h={15}
            mt={8}
            style={{ borderRadius: 6, zIndex: 10 }}
          />
        </Stack>
      </CustomSkeleton>
    </>
  );
};
export default JobListSkeletonCard;
