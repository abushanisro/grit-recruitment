import { useDisclosure } from "@mantine/hooks";
import { AppShell, Burger, Group } from "@mantine/core";
import Header from "@components/Header";

const PageLayout = ({
  pageBg = "linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)",
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
        px={{ base: 16, sm: 24, md: 40 }}
        bg={"primaryDarkBlue.9"}
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
        px={{ base: 16, sm: 24, md: 40 }}
        c="white"
        pb={{ base: 24, md: 32, lg: 40 }}
      >
        {mainComponent}
      </AppShell.Main>
    </AppShell>
  );
};
export default PageLayout;
