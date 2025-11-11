import CustomSkeleton from "@components/CustomSkeleton";

const ProfileLoader = () => {
  return (
    <CustomSkeleton
      height={600}
      radius="xl"
      mx="auto"
      maw={{ base: 370, xs: 600, sm: 700, md: 800, lg: 1100, xl: 1200 }}
      mt={{ base: 40, xl: 100 }}
    />
  );
};

export default ProfileLoader;
