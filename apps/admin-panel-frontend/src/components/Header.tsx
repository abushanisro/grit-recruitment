import { Group, Image } from "@mantine/core";
import Link from "next/link";
import { useLogin } from "@/contexts/LoginProvider";
import LoggedInHeaderPanel from "@/components/LoggedInHeaderPanel";
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
          src="/images/logo.png"
          alt="logo"
          w={{ base: 200, sm: 260, xl: 300 }}
          h={{ base: 15, sm: 20, xl: 23 }}
        />
      </Link>
      <Group gap={20} display={{ base: "none", sm: "flex" }}>
        {isLoggedIn ? <LoggedInHeaderPanel /> : <NotLoggedInHeaderPanel />}
      </Group>
    </Group>
  );
};
export default Header;
