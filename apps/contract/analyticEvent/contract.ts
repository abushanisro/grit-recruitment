import { initContract } from "@ts-rest/core";
import { analyticEventDto } from "./types";
import { SuccessSchema } from "contract/common";

const c = initContract();

export const analyticEventContract = c.router(
  {
    createAnalyticEvent: {
      method: "POST",
      path: "/analyticEvents",
      responses: {
        200: SuccessSchema,
      },
      body: analyticEventDto,
    },
  },
  {
    pathPrefix: "/analytics",
  }
);
