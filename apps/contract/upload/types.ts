import { z } from "zod";

export const uploadSuccess = z.object({
  url: z.string(),
  key: z.string(),
  type: z.string(),
  isUploaded: z.boolean(),
  message: z.string(),
});
