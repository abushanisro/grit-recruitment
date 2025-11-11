import type { AppProps } from "next/app";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MantineProvider } from "@mantine/core";
import { Notifications } from "@mantine/notifications";
import { ModalsProvider } from "@mantine/modals";
import { useMediaQuery } from "@mantine/hooks";
import { myTheme } from "theme";
import "@mantine/core/styles.css";
import "@mantine/notifications/styles.css";
import { PageLoaderProvider } from "@/contexts/PageLoaderProvider";
import "@mantine/dates/styles.css";
import { LoginProvider } from "@/contexts/LoginProvider";
import { UserDataProvider } from "@/contexts/UserProvider";

const queryClient = new QueryClient();

export default function App({ Component, pageProps }: AppProps) {
  const isMobile = useMediaQuery("(max-width: 425px)");
  return (
    <>
      <MantineProvider theme={myTheme}>
        <ModalsProvider>
          <Notifications
            limit={3}
            position={isMobile ? "bottom-center" : "bottom-right"}
            autoClose={4000}
          />
          <PageLoaderProvider>
            <QueryClientProvider client={queryClient}>
              <LoginProvider>
                <UserDataProvider>
                  <Component {...pageProps} />
                </UserDataProvider>
              </LoginProvider>
            </QueryClientProvider>
          </PageLoaderProvider>
        </ModalsProvider>
      </MantineProvider>
    </>
  );
}
