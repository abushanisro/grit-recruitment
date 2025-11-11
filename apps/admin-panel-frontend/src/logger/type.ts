export interface LogInterface {
  timestamp: number;
  level: LogLevelEnum;
  type: ErrorTypeEnum;
  body: BodyInterface;
  project: string;
  browserName: string;
  browserVersion: string;
  osName: string;
  osVersion: string;
  userAgent: string;
  deviceType: DeviceTypeEnum;
}
export interface BodyInterface {
  request: RequestInterface;
  response: ResponsePayloadInterface;
}
export interface RequestInterface {
  method: string;
  headers: Record<string, string>;
  body: Record<string, string>;
  url: string;
  queryParams: Record<string, string>;
}
export interface RequestBodyInterface {
  method: string;
  headers: Record<string, string>;
  body: Record<string, string>;
  url: URL;
}
export interface ResponseInterface {
  status: number;
  headers: Headers;
  body: unknown;
}
export interface ResponsePayloadInterface {
  status: number;
  headers: Record<string, string>;
  body: unknown;
}

export enum DeviceTypeEnum {
  mobile = "mobile",
  tablet = "tablet",
  desktop = "desktop",
}

export enum LogLevelEnum {
  info = "info",
  warn = "warn",
  error = "error",
}
export enum ErrorTypeEnum {
  window = "window",
  console = "console",
  api = "api",
}

export interface LogRequestFunctionInterface {
  (
    url: string,
    options: { method?: string; data?: any },
    reqHeaders: Record<string, string>,
    res: {
      status: number;
      body: unknown;
      headers: Headers;
    }
  ): Promise<void>;
}
