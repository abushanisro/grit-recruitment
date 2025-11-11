import { z } from "zod";

export const commonDto = z.object({
  firstName: z.string().min(3),
  lastName: z.string().min(1),
  email: z.string().email(),
  phone: z.string().length(10),
  picture: z.string().nullable(),
});
