import {
  Text,
  Box,
  Image,
  BoxProps,
  ImageProps,
  TextProps,
  FlexProps,
  Flex,
} from "@mantine/core";

export enum TextWithArrowOrderEnum {
  image = "image",
  text = "text",
}

const StyledText = ({ label, ...props }: { label: string } & TextProps) => {
  return (
    <Text
      ta="center"
      ff="Gaegu"
      fw="700"
      lh="1.25"
      fz={{ base: 18, md: 30 }}
      maw={{ base: 100, md: 200 }}
      {...props}
    >
      {label}
    </Text>
  );
};

const TextWithArrowComponent = ({
  label,
  imageProps,
  imageContainerProps,
  textProps,
  elementRenderOrder = [
    TextWithArrowOrderEnum.text,
    TextWithArrowOrderEnum.image,
  ],
  ...props
}: {
  label: string;
  imageProps?: ImageProps;
  imageContainerProps?: FlexProps;
  textProps?: TextProps;
  elementRenderOrder?: TextWithArrowOrderEnum[];
} & BoxProps) => {
  const [firstItem, secondItem] = elementRenderOrder;
  return (
    <Box
      pos="absolute"
      left={{ base: "-90px", md: "-130px" }}
      top={{ base: "-60px", md: "-120px" }}
      {...props}
    >
      {firstItem === TextWithArrowOrderEnum.text && (
        <StyledText label={label} {...textProps} />
      )}
      <Flex align="center" justify="center" {...imageContainerProps}>
        <Image
          src="/images/curvedArrow.svg"
          alt="arrow"
          w={{ base: 20, md: 37.5 }}
          h={{ base: 20, md: 37.5 }}
          mt={14}
          {...imageProps}
        />
      </Flex>
      {secondItem === TextWithArrowOrderEnum.text && (
        <StyledText label={label} {...textProps} />
      )}
    </Box>
  );
};
export default TextWithArrowComponent;
