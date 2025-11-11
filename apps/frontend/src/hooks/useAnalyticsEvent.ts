import { getOriginUrl } from "@/utils/analytics";
import { getQueryClient } from "api";
import { useApi } from "@/hooks/useApi";
import { browserName, deviceType } from "react-device-detect";

export const useAnalyticsEvent = () => {
  const { makeApiCall } = useApi();
  const sendEvent = (eventName: string) => {
    const originUrl = getOriginUrl();
    makeApiCall({
      fetcherFn: async () => {
        const response =
          await getQueryClient().analyticEvent.createAnalyticEvent.mutation({
            body: {
              pageUrl: originUrl,
              device: deviceType,
              browser: browserName,
              userAgent:
                typeof window !== "undefined"
                  ? window?.navigator?.userAgent
                  : "",
              eventName: eventName,
            },
          });
        return response;
      },
      showLoader: false,
      showFailureMsg: false,
    });
  };

  return { sendEvent };
};