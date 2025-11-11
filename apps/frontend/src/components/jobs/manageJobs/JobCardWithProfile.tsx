import { Stack, Avatar } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";

const JobCardWithProfile = ({
  image,
  children,
  showImage = true,
}: {
  image?: string;
  showImage?: boolean;
  children: React.ReactNode;
}) => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  const isTablet = useMediaQuery("(max-width: 768px)");
  return (
    <Stack
      pos="relative"
      bg="primaryDarkBlue.9"
      px={isMobile ? 30 : isTablet ? 40 : 60}
      py={isMobile ? 30 : isTablet ? 40 : 50}
      my={isMobile ? 10 : isTablet ? 15 : 20}
      style={{
        borderRadius: isTablet ? 16 : 38,
        border: "1px solid",
        borderColor: "var(--mantine-color-primaryGreen-1)",
        boxShadow: "0px 4px 4px 0px var(--mantine-color-primaryGreen-3)",
      }}
      gap={0}
    >
      {showImage && (
        <Avatar
          pos="absolute"
          w={isMobile ? 40 : isTablet ? 50 : 80}
          h={isMobile ? 40 : isTablet ? 50 : 80}
          left={isMobile ? -20 : isTablet ? -25 : -40}
          bg="customGray.2"
          style={{
            border: "1px solid",
            borderColor: "var(--mantine-color-primaryGreen-3)",
          }}
          src={image}
          alt="company logo"
        />
      )}
      {children}
    </Stack>
  );
};
export default JobCardWithProfile;
