import { useDisclosure } from "@mantine/hooks";
import { AppShell, Burger, Group } from "@mantine/core";
import Header from "@components/Header";

const PageLayout = ({
  pageBg = "customBlack.4",
  navbarComponent,
  headerComponent,
  mainComponent,
}: {
  pageBg?: string;
  navbarComponent?: React.ReactNode;
  headerComponent?: React.ReactNode;
  mainComponent: React.ReactNode;
}) => {
  const [opened, { toggle }] = useDisclosure();
  return (
    <AppShell
      header={{ height: { base: 60, sm: 80 } }}
      bg={pageBg}
      navbar={{
        width: 300,
        breakpoint: "sm",
        collapsed: { desktop: true, mobile: !opened },
      }}
    >
      <AppShell.Header
        mx={{ base: 20, md: 0 }}
        px={{ base: 0, md: 80 }}
        pt={{ base: 20, md: 0 }}
        bg={pageBg}
        withBorder={false}
      >
        {headerComponent ? (
          headerComponent
        ) : (
          <Group h="100%">
            <Burger
              aria-label="Toggle menu"
              opened={opened}
              onClick={toggle}
              hiddenFrom="sm"
              size="sm"
              color="white"
            />
            <Header />
          </Group>
        )}
      </AppShell.Header>
      <AppShell.Navbar py="md" px={20} bg={pageBg}>
        {navbarComponent}
      </AppShell.Navbar>
      <AppShell.Main
        px={{ base: 30, md: 80 }}
        c="white"
        pb={{ base: 40, sm: 60, md: 100 }}
      >
        {mainComponent}
      </AppShell.Main>
    </AppShell>
  );
};
export default PageLayout;
