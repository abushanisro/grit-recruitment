import { AspectRatio, Box, Center,Loader } from "@mantine/core";
import React from "react";
import { useRouter } from "next/router";
import { useMediaQuery } from "@mantine/hooks";

const PlayVideoResumeComponent = () => {
  const isMobile = useMediaQuery("(max-width: 425px)");
  const router = useRouter();
    const videoUrl = String(router.query.videoUrl ?? "");
  if (!videoUrl) {
    return (
      <Center
        bg="linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"
        h="100vh"
        w="100%"
      >
        <Loader />
      </Center>
    );
  } else {
    return (
      <Box w="100%" h="100vh">
        <Box h={"100%"} my={20} mx={20}> 
         <AspectRatio ratio={isMobile ? 16 / 10 : 16 / 7}>
              <video controls>
                <source src={videoUrl} type="video/mp4" />
              </video>
            </AspectRatio>
         
        </Box>
      </Box>
    );
  }
};

export default PlayVideoResumeComponent;
