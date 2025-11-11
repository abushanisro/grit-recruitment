import { initContract } from "@ts-rest/core";
import { SuccessSchema } from "contract/common";
import { z } from "zod";

const c = initContract();

export const EmailLoginSchema = z.object({
  email: z.string().email(),
});

export const googleUserDataSchema = z.object({
  firstName: z.string(),
  lastName: z.string(),
  email: z.string(),
  referenceId: z.string(),
  picture: z.string(),
});

export const authContract = c.router(
  {
    sendInvite: {
      method: "POST",
      path: "/sendInvite",
      responses: {
        200: SuccessSchema,
      },
      body: EmailLoginSchema,
    },
    getGoogleUser: {
      method: "GET",
      path: "/getGoogleUser",
      responses: {
        200: googleUserDataSchema,
      },
      query: z.object({
        refId: z.string(),
      }),
    },
  },
  {
    pathPrefix: "/auth",
  }
);
