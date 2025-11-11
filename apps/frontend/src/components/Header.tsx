import { Group, Image } from "@mantine/core";
import Link from "next/link";
import LoggedInHeaderPanel from "@/components/LoggedInHeaderPanel";
import { useLogin } from "@/contexts/LoginProvider";
import NotLoggedInHeaderPanel from "@/components/NotLoggedInHeaderPanel";

const Header = () => {
  const { isLoggedIn } = useLogin();
  return (
    <Group
      justify="space-between"
      w={{ base: "80%", sm: "100%" }}
      align="center"
    >
      <Link href="/">
        <Image
          src="/images/mainLogo.png"
          alt="logo"
          w={{ base: 200, sm: 260, xl: 300 }}
          h={{ base: 20, sm: 24, lg: 28, xl: 32 }}
        />
      </Link>
      <Group gap={20} display={{ base: "none", sm: "flex" }}>
        {isLoggedIn ? <LoggedInHeaderPanel /> : <NotLoggedInHeaderPanel />}
      </Group>
    </Group>
  );
};
export default Header;
