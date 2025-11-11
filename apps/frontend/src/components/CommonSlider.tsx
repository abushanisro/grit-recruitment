import { Box, Flex } from "@mantine/core";
import useEmblaCarousel from "embla-carousel-react";
import { useCallback, useEffect, useState } from "react";

export const SliderContainer = ({
  children,
  emblaRef,
}: {
  children: React.ReactNode;
  emblaRef: any;
}) => {
  return (
    <Box style={{ overflow: "hidden" }} ref={emblaRef}>
      <Flex>{children}</Flex>
    </Box>
  );
};

const CommonSlider = ({
  disableDrag,
  renderBody,
}: {
  disableDrag: boolean;
  renderBody: (
    ref: any,
    scrollPrev: () => void,
    scrollNext: () => void,
    prevBtnEnabled: boolean,
    nextBtnEnabled: boolean
  ) => JSX.Element;
}) => {
  const [emblaRef, embla] = useEmblaCarousel({
    align: "start",
    containScroll: "trimSnaps",
    watchDrag: !disableDrag,
  });

  const scrollPrev = useCallback(() => embla && embla.scrollPrev(), [embla]);
  const scrollNext = useCallback(() => embla && embla.scrollNext(), [embla]);
  const [prevBtnEnabled, setPrevBtnEnabled] = useState(false);
  const [nextBtnEnabled, setNextBtnEnabled] = useState(false);

  const onSelect = useCallback(() => {
    if (!embla) return;
    setPrevBtnEnabled(embla.canScrollPrev());
    setNextBtnEnabled(embla.canScrollNext());
  }, [embla]);

  useEffect(() => {
    if (!embla) return;
    onSelect();
    embla.on("select", onSelect);
  }, [embla, onSelect]);

  return renderBody(
    emblaRef,
    scrollPrev,
    scrollNext,
    prevBtnEnabled,
    nextBtnEnabled
  );
};

export default CommonSlider;
