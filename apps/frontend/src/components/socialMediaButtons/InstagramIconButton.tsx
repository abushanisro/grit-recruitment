import { IconBrandInstagram } from "@tabler/icons-react";
import PrimaryIconButton from "@/components/buttons/PrimaryIconButton";
import { Center } from "@mantine/core";

const InstagramIconButton = () => {
  return (
    <Center>
      <PrimaryIconButton w={33} h={33}>
        <IconBrandInstagram color="black" size={28} />
      </PrimaryIconButton>
    </Center>
  );
};
export default InstagramIconButton;
