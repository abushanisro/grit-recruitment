import { IconMailFilled } from "@tabler/icons-react";
import PrimaryIconButton from "@/components/buttons/PrimaryIconButton";
import { Center} from "@mantine/core";

const EmailIconButton = () => {
  return (
    <Center>
      <PrimaryIconButton w={33} h={33} c="black">
        <IconMailFilled fill="currentColor" size={26} />
      </PrimaryIconButton>
    </Center>
  );
};
export default EmailIconButton;
