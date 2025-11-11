import { getApiUrl } from "@/env";
import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
import Cookies from "js-cookie";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import {
  DateFilterEnum,
  ExpectedSalaryInLpa,
  ExperienceInYear,
  NoticePeriodEnum,
} from "contract/enum";
dayjs.extend(relativeTime);
dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.tz.guess();

export const postHogApiKey: string =
  "phc_XJ2pdrlY8RAGqkXtRwavmBHAm7VGaiRp4LRZ93ivwhL";

export const razorPayKey: string = "rzp_test_XPm0ztcFBMSVbk";

export const getTimeFromNow = (updatedTime: Date) => {
  return dayjs(updatedTime).fromNow();
};
export const getIndianDate = (updatedTime: string) => {
  const currentDate = dayjs(updatedTime).tz("Asia/Kolkata");
  return currentDate.format("DD/MM/YYYY,  h:mm:ss a");
};

export function typedKeys<T extends {}>(obj: T) {
  return Object.keys(obj) as unknown as (keyof T)[];
}

export const convertEnumToLabel = (enumValue: string): string => {
  // Split camelCase and join with space
  return enumValue.replace(/([a-z])([A-Z])/g, "$1 $2");
};

export const logout = () => {
  if (Cookies.get("userToken")) {
    Cookies.remove("userToken");
  }
  window.location.href = "/?ua=true";
};
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
export const loginViaGoogleWithRedirectTarget = ({
  pathname,
  redirectUrl,
}: {
  pathname: string;
  redirectUrl: string;
}) => {
  if (pathname === "/") {
    window.open(`${getApiUrl()}/google`, "_self");
    return;
  }
  window.open(
    `${getApiUrl()}/google?redirectUrl=${encodeURIComponent(redirectUrl)}`,
    "_self"
  );
};

export const getFullName = (firstName: string, lastName: string) => {
  return `${firstName} ${lastName}`;
};

export const getNoticePeriodLabel = (period: NoticePeriodEnum): string => {
  switch (period) {
    case NoticePeriodEnum.immediately:
      return "Immediately";
    case NoticePeriodEnum.twoWeeks:
      return "2 Weeks";
    case NoticePeriodEnum.oneMonth:
      return "1 Month";
    case NoticePeriodEnum.sixMonths:
      return "6 Months";
    case NoticePeriodEnum.moreThanSixMonths:
      return "6+ Months";
    default:
      return "";
  }
};

export const getExperienceLabel = (experience: ExperienceInYear): string => {
  switch (experience) {
    case ExperienceInYear.MoreThanTenYears:
      return "More than 10 years";
    default:
      return experience;
  }
};
export function getDateFromEnum(value: DateFilterEnum) {
  const date = new Date();
  switch (value) {
    case DateFilterEnum.Today:
      date.setDate(date.getDate() - 1);
      return String(date);
    case DateFilterEnum.ThisWeek:
      date.setDate(date.getDate() - 7);
      return String(date);
    case DateFilterEnum.ThisMonth:
      date.setMonth(date.getMonth() - 1);
      return String(date);
    default:
      return "custom";
  }
}

export function getDateLabelEnum(value: DateFilterEnum) {
  switch (value) {
    case DateFilterEnum.Today:
      return "Today";
    case DateFilterEnum.ThisWeek:
      return "This Week";
    case DateFilterEnum.ThisMonth:
      return "This Month";
    default:
      return "Custom";
  }
}
export const getCTCLabel = (ctc: ExpectedSalaryInLpa): string => {
  switch (ctc) {
    case ExpectedSalaryInLpa.MoreThanTwelveLPA:
      return "More than 12 LPA";
    default:
      return ctc;
  }
};

export const appendQueryToRedirectUrl = ({
  url,
  query,
  queryValue,
}: {
  url: string;
  query: string;
  queryValue: string;
}) => {
  if (!url.includes("?")) {
    return `${url}?${query}=${queryValue}`;
  }
  return `${url}&${query}=${queryValue}`;
};
