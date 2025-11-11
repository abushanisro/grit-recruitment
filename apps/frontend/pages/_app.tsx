import "../styles/globals.css";
import { useEffect } from "react";
import type { AppProps } from "next/app";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MantineProvider } from "@mantine/core";
import { Notifications } from "@mantine/notifications";
import { ModalsProvider } from "@mantine/modals";
import { useMediaQuery } from "@mantine/hooks";
import { myTheme } from "theme";
import "@mantine/core/styles.css";
import { LoginProvider } from "@/contexts/LoginProvider";
import "@mantine/notifications/styles.css";
import { PageLoaderProvider } from "@/contexts/PageLoaderProvider";
import { UserDataProvider } from "@/contexts/UserProvider";
import "@mantine/dates/styles.css";
import { PageAuthenticationChecker } from "@components/PageAuthenticationChecker";
import { postHogApiKey } from "@/utils/common";
import Script from "next/script";


const queryClient = new QueryClient();

export default function App({ Component, pageProps }: AppProps) {
  useEffect(() => {
    import("posthog-js").then((posthog) => {
      posthog.default.init(postHogApiKey, {
        api_host: "https://app.posthog.com",
      });
    });

  }, []);
  const isMobile = useMediaQuery("(max-width: 425px)");
  return (
    <>
      <Script
        strategy="lazyOnload"
        id="googleAnalyticsScript"
        async
        src="https://www.googletagmanager.com/gtag/js?id=G-R5VBVF4X08"
      />
      <Script strategy="lazyOnload" id="googleAnalyticsScript2">
        {`
                    window.dataLayer = window.dataLayer || [];
                    function gtag(){dataLayer.push(arguments);}
                    gtag('js', new Date());
                    gtag('config', 'G-R5VBVF4X08', {
                    page_path: window.location.pathname,
                    });
                `}
      </Script>{" "}
      <MantineProvider theme={myTheme} >
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
                  <PageAuthenticationChecker>
                    <Component {...pageProps} />
                  </PageAuthenticationChecker>
                </UserDataProvider>
              </LoginProvider>
            </QueryClientProvider>
          </PageLoaderProvider>
        </ModalsProvider>
      </MantineProvider>
    </>
  );
}
