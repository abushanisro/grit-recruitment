import { initContract } from "@ts-rest/core";
import { SuccessSchema } from "contract/common";
import { paginationDto } from "contract/job/types";
import {
  activeSubscriptionsResponse,
  boostJobBody,
  getBoostedJobsForSubscriptionResponse,
  getLastPaymentDetailsResponse,
  getOrderBody,
  getOrderResponse,
  subscriptionHistoryResponse,
  subscriptionPlansResponse,
  updateSubscriptionBody,
} from "contract/paidJobs/types";
import { createPaginatedResponseSchema } from "contract/utils";
import { z } from "zod";

const c = initContract();

export const paidJobsContract = c.router(
  {
    getLastPaymentDetails: {
      method: "GET",
      path: "/getLastPaymentDetails",
      responses: {
        200: getLastPaymentDetailsResponse,
      },
    },
    getSubscriptionPlans: {
      method: "GET",
      path: "/getSubscriptionPlans",
      responses: {
        200: subscriptionPlansResponse.array(),
      },
    },

    getOrder: {
      method: "POST",
      path: "/getOrder",
      responses: {
        201: getOrderResponse,
      },
      body: getOrderBody,
    },

    updateSubscription: {
      method: "POST",
      path: "/updateSubscription",
      responses: {
        201: SuccessSchema,
      },
      body: updateSubscriptionBody,
    },

    updateSubscriptionFromJobForm: {
      method: "POST",
      path: "/updateSubscriptionFromJobForm",
      responses: {
        201: SuccessSchema,
      },
      body: updateSubscriptionBody,
    },

    getSubscriptionHistory: {
      method: "GET",
      path: "/getSubscriptionHistory",
      responses: {
        200: createPaginatedResponseSchema(subscriptionHistoryResponse),
      },
      query: paginationDto,
    },

    getActiveSubscriptions: {
      method: "GET",
      path: "/getActiveSubscriptions",
      responses: {
        200: createPaginatedResponseSchema(activeSubscriptionsResponse),
      },
      query: paginationDto,
    },
    getBoostedJobsForSubscription: {
      method: "GET",
      path: "/getBoostedJobsForSubscription",
      responses: {
        201: createPaginatedResponseSchema(
          getBoostedJobsForSubscriptionResponse
        ),
      },
      query: z
        .object({
          subscriptionId: z.string().transform(Number),
        })
        .extend(paginationDto.shape),
    },

    boostJob: {
      method: "POST",
      path: "/boostJob",
      responses: {
        201: SuccessSchema,
      },
      body: boostJobBody,
    },
  },
  { pathPrefix: "/paidJobs" }
);
