import pino from "pino";
const pinoTransmitHttp = require("pino-transmit-http");

import {
  isMobile,
  isTablet,
  browserName,
  browserVersion,
  osName,
  osVersion,
} from "react-device-detect";
import {
  DeviceTypeEnum,
  ErrorTypeEnum,
  LogInterface,
  LogLevelEnum,
  LogRequestFunctionInterface,
  RequestBodyInterface,
  ResponseInterface,
} from "@/logger/type";

const convertHeadersToObject = (headers: Headers): Record<string, string> => {
  const headersObj: Record<string, string> = {};
  headers.forEach((value, key) => {
    if (value !== undefined) {
      headersObj[key.toLowerCase()] = Array.isArray(value)
        ? value.join(", ")
        : value;
    }
  });
  return headersObj;
};

const getLogType = (statusCode: number) => {
  if (statusCode > 199 && statusCode < 300) {
    return LogLevelEnum.info;
  }
  if (statusCode > 399 && statusCode < 600) {
    return LogLevelEnum.error;
  }
  return LogLevelEnum.info;
};

export const createLogger = ({
  projectId,
  disableLogs,
}: {
  projectId: string;
  disableLogs: boolean;
}) => {
  const transmitHttp = pinoTransmitHttp({
    url: "http://localhost:3002", // NOTE : Clickhouse Logger Service should api url
    headers: {
      "content-type": "application/json",
    },
    throttle: 2000, //Amount of milliseconds to throttle the transmission of the log messages
  });
  const logger = pino({
    level: "info",
    browser: {
      disabled: true,
      asObject: true,
      serialize: true,
      transmit: {
        level: transmitHttp.level,
        send: function (log, logEvent) {
          if (logEvent.messages.length > 0) {
            transmitHttp.send(log, logEvent.messages[0]);
          }
        },
      },
    },
  });
  const getErrorPayload = (errorMessagePayload: any, type: ErrorTypeEnum) => {
    return {
      timestamp: Math.round(Date.now() / 1000),
      level: LogLevelEnum.error,
      type,
      body: errorMessagePayload,
      project: projectId,
      browserName,
      browserVersion,
      osName,
      osVersion,
      userAgent:
        typeof window !== "undefined" ? window?.navigator?.userAgent : "",
      deviceType: isMobile
        ? DeviceTypeEnum.mobile
        : isTablet
        ? DeviceTypeEnum.tablet
        : DeviceTypeEnum.desktop,
    };
  };
  const defaultConsoleErrorLoggerFn = console.error;
  console.error = (msg, ...args) => {
    if (!disableLogs) {
      logger.error(
        getErrorPayload({ message: msg, args }, ErrorTypeEnum.console)
      );
    }
    defaultConsoleErrorLoggerFn(msg, ...args);
  };
  if (typeof window !== "undefined") {
    window.onerror = function (
      message: string | Event | Error,
      source: string | undefined,
      lineno: number | undefined,
      colno: number | undefined,
      error: Error | undefined
    ): boolean {
      logWindowError(
        getErrorPayload(
          {
            message:
              typeof message === "string"
                ? message
                : (message as Error).message,
            stack: error?.stack ?? {},
            source: source ?? "",
            lineNo: lineno ?? 0,
            colNo: colno ?? 0,
          },
          ErrorTypeEnum.window
        )
      );
      return false;
    };
  }
  const getPayload = (
    request: RequestBodyInterface,
    response: ResponseInterface
  ): LogInterface => {
    const queryParams = Object.fromEntries(request.url.searchParams.entries());
    const urlWithoutParams = request.url.origin + request.url.pathname;
    return {
      timestamp: Math.round(Date.now() / 1000),
      level: getLogType(response.status),
      type: ErrorTypeEnum.api,
      body: {
        request: {
          method: request.method,
          headers: request.headers,
          body: request.body,
          url: urlWithoutParams,
          queryParams: queryParams,
        },
        response: {
          status: response.status,
          headers: convertHeadersToObject(response.headers),
          body: response.body,
        },
      },
      project: projectId,
      browserName,
      browserVersion,
      osName,
      osVersion,
      userAgent:
        typeof window !== "undefined" ? window?.navigator?.userAgent : "",
      deviceType: isMobile
        ? DeviceTypeEnum.mobile
        : isTablet
        ? DeviceTypeEnum.tablet
        : DeviceTypeEnum.desktop,
    };
  };

  const logRequest: LogRequestFunctionInterface = async (
    url: string,
    options: { method?: string; data?: any },
    reqHeaders: Record<string, string>,
    res: {
      status: number;
      body: unknown;
      headers: Headers;
    }
  ) => {
    if (disableLogs) {
      return;
    }

    const urlObj = new URL(url);
    const logPayload = getPayload(
      {
        method: options.method ?? "GET",
        headers: reqHeaders,
        body: options.data,
        url: urlObj,
      },
      {
        status: res.status,
        headers: res.headers,
        body: res.body,
      }
    );

    if (logPayload.level === LogLevelEnum.error) {
      logger.error(logPayload);
    } else {
      logger.info(logPayload);
    }
  };

  const logWindowError = (errorPayload: any) => {
    if (disableLogs) {
      return;
    }
    logger.error(errorPayload);
  };

  return { logRequest };
};
