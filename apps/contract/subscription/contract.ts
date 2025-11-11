import { initContract } from "@ts-rest/core";
import {
  activeSubscriptionsResponse,
  employerProfileViewBody,
  employerProfileViewResponse,
  filterDirectoryBody,
  filterDirectoryResponse,
  getAllJobsFromEmployerResponse,
  getEmployerProfileViewBody,
  getEmployerProfileViewResponse,
  getJobSeekerProfileQuery,
  getLastPaymentDetailsResponse,
  getOrderBody,
  getOrderResponse,
  getSubscriptionPlansQuery,
  getViewedUsersForSubscriptionResponse,
  inviteUsersBody,
  subscriptionHistoryResponse,
  subscriptionPlansResponse,
  suggestionActiveSubscriptionsResponse,
  updateSubscriptionBody,
} from "./types";
import { SuccessSchema } from "contract/common";
import { paginationDto } from "contract/job/types";
import { jobSeekerResponse } from "contract/user/types";
import { createPaginatedResponseSchema } from "contract/utils";
import { z } from "zod";

const c = initContract();

export const subscriptionContract = c.router(
  {
    getSubscriptionPlans: {
      method: "GET",
      path: "/getSubscriptionPlans",
      responses: {
        200: subscriptionPlansResponse.array(),
      },
      query: getSubscriptionPlansQuery,
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

    getSubscriptionHistory: {
      method: "GET",
      path: "/getSubscriptionHistory",
      responses: {
        200: createPaginatedResponseSchema(subscriptionHistoryResponse),
      },
      query: paginationDto,
    },
    
    getJobSeekerProfile: {
      method: "GET",
      path: "/getJobSeekerProfile",
      responses: {
        200: jobSeekerResponse,
      },
      query: getJobSeekerProfileQuery,
    },

    getEmployerProfileView: {
      method: "POST",
      path: "/getEmployerProfileView",
      responses: {
        201: getEmployerProfileViewResponse,
      },
      body: getEmployerProfileViewBody,
    },

    getActiveSubscriptions: {
      method: "GET",
      path: "/getActiveSubscriptions",
      responses: {
        200: createPaginatedResponseSchema(activeSubscriptionsResponse),
      },
      query: paginationDto,
    },

    suggestionActiveSubscriptions: {
      method: "GET",
      path: "/suggestionActiveSubscriptions",
      responses: {
        200: suggestionActiveSubscriptionsResponse,
      },
    },

    filterDirectory: {
      method: "GET",
      path: "/filterJobSeekerDirectory",
      responses: {
        200: createPaginatedResponseSchema(filterDirectoryResponse),
      },
      query: filterDirectoryBody.extend(paginationDto.shape),
    },

    inviteUsers: {
      method: "POST",
      path: "/inviteJobSeekerForJob",
      responses: {
        201: SuccessSchema,
      },
      body: inviteUsersBody,
    },

    getAllJobsFromEmployer: {
      method: "GET",
      path: "/getAllJobsFromEmployer",
      responses: {
        200: createPaginatedResponseSchema(getAllJobsFromEmployerResponse),
      },
      query: z
        .object({
          searchText: z.string().optional(),
        })
        .extend(paginationDto.shape),
    },

    viewJobSeekersProfile: {
      method: "GET",
      path: "/viewJobSeekersProfile",
      responses: {
        200: jobSeekerResponse,
      },
      query: getJobSeekerProfileQuery,
    },

    employerProfileView: {
      method: "POST",
      path: "/employerProfileView",
      body: employerProfileViewBody,
      responses: {
        201: employerProfileViewResponse,
      },
    },

    getLastPaymentDetails: {
      method: "GET",
      path: "/getLastPaymentDetails",
      responses: {
        200: getLastPaymentDetailsResponse,
      },
    },

    getViewedUsersForSubscription: {
      method: "GET",
      path: "/getViewedUsersForSubscription",
      responses: {
        201: createPaginatedResponseSchema(
          getViewedUsersForSubscriptionResponse
        ),
      },
      query: z
        .object({
          subscriptionId: z.string().transform(Number),
        })
        .extend(paginationDto.shape),
    },
  },
  {
    pathPrefix: "/subscriptions",
  }
);
