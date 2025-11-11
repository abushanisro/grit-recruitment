import { createContext, useContext } from "react";
import PageLoader from "@/components/PageLoader";
import { useDisclosure } from "@mantine/hooks";

interface ContextInterface {
  opened: boolean;
  showPageLoader: () => void;
  hidePageLoader: () => void;
}

const PageLoaderContext = createContext<ContextInterface>({
  opened: false,
  showPageLoader: () => {},
  hidePageLoader: () => {},
});

export const PageLoaderProvider = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const [opened, { open, close }] = useDisclosure(false);

  const showPageLoader = () => {
    open();
  };

  const hidePageLoader = () => {
    close();
  };

  return (
    <PageLoaderContext.Provider
      value={{
        opened,
        showPageLoader,
        hidePageLoader,
      }}
    >
      <PageLoader opened={opened} onClose={hidePageLoader} />
      {children}
    </PageLoaderContext.Provider>
  );
};

export const usePageLoader = () => useContext(PageLoaderContext);
