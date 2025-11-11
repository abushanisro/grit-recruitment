import { getEnv, getProjectId } from "@/env";
import { createLogger } from "@/logger/centralizedLogger";

export const centralizedLogger = () => {
  return createLogger({
    projectId: getProjectId(),
    disableLogs: getEnv() === "dev",
  });
};
