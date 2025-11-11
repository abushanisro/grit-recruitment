import { z } from "zod";

export const analyticEventDto = z.object({
  eventName: z.string(),
  pageUrl: z.string(),
  device: z.string(),
  browser: z.string(),
  userAgent: z.string(),
});
