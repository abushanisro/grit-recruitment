import { initQueryClient } from "@ts-rest/react-query";
import { getApiUrl } from "@/env";
import { contract } from "contract";
import Cookies from "js-cookie";
import { tsRestFetchApi } from "@ts-rest/core";
import { centralizedLogger } from "@/commonLogger";

const { logRequest } = centralizedLogger();

const parseBody = (body: string | FormData | null | undefined) => {
  if (typeof body === "string") {
    try {
      return JSON.parse(body);
    } catch {
      return body;
    }
  }
  return body;
};

const queryClient = (authToken: string | undefined) =>
  initQueryClient(contract, {
    baseUrl: getApiUrl(),
    baseHeaders: authToken ? { Authorization: `Bearer ${authToken}` } : {},
    api: async (args) => {
      const response = await tsRestFetchApi(args);
      logRequest(
        args.path,
        {
          method: args.method,
          data: parseBody(args.body),
        },
        args.headers,
        response
      );
      if (response.status === 401) {
        Cookies.remove("userToken");
        window.location.href = "/?ua=true";
      }
      return response;
    },
  });
export const getQueryClient = () => {
  const authToken = Cookies.get("userToken");
  return queryClient(authToken);
};
