import { getApiUrl } from "@/env";
import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
import Cookies from "js-cookie";
dayjs.extend(relativeTime);

export const postHogApiKey: string =
  "phc_XJ2pdrlY8RAGqkXtRwavmBHAm7VGaiRp4LRZ93ivwhL";

export const getTimeFromNow = (updatedTime: Date) => {
  return dayjs(updatedTime).fromNow();
};

export function typedKeys<T extends {}>(obj: T) {
  return Object.keys(obj) as unknown as (keyof T)[];
}
export const loginViaGoogle = (pathname: string) => {
  if (pathname === "/") {
    window.open(`${getApiUrl()}/google`, "_self");
    return;
  }
  window.open(
    `${getApiUrl()}/google?redirectUrl=${encodeURIComponent(
      window.location.href
    )}`,
    "_self"
  );
};
export const logout = () => {
  if (Cookies.get("userToken")) {
    Cookies.remove("userToken");
  }
  window.location.href = "/?ua=true";
};
