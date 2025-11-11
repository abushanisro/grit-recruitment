import { initContract } from "@ts-rest/core";
import {
  UserCreationSuccessSchema,
  UserProfileDto,
  employerDto,
  employerResponse,
  jobSeekerDto,
  jobSeekerResponse,
} from "./types";
import { SuccessSchema } from "contract/common";
import { z } from "zod";

const c = initContract();

export const userContract = c.router(
  {
    createJobseeker: {
      method: "POST",
      path: "/createJobseeker",
      responses: {
        200: UserCreationSuccessSchema,
      },
      body: jobSeekerDto,
    },

    createEmployer: {
      method: "POST",
      path: "/createEmployer",
      responses: {
        200: UserCreationSuccessSchema,
      },
      body: employerDto,
    },

    updateJobseeker: {
      method: "PUT",
      path: "/updateJobseeker",
      responses: {
        200: SuccessSchema,
      },
      body: jobSeekerDto,
    },

    updateEmployer: {
      method: "PUT",
      path: "/updateEmployer",
      responses: {
        200: SuccessSchema,
      },
      body: employerDto,
    },

    getJobseekerProfile: {
      method: "GET",
      path: "/getJobseekerProfile",
      responses: {
        200: jobSeekerResponse,
      },
    },

    getEmployerById: {
      method: "GET",
      path: "/getEmployerDetail",
      responses: {
        200: employerResponse,
      },
    },
    getUserProfile: {
      method: "GET",
      path: "/getUserProfile",
      responses: {
        200: UserProfileDto,
      },
    },

    viewJobseekerProfileFromManageApplication: {
      method: "GET",
      path: "/viewJobseekerProfileFromManageApplication",
      responses: {
        200: jobSeekerResponse,
      },
      query: z.object({
        jobApplicationId: z.string().transform(Number),
      }),
    },

  },

  {
    pathPrefix: "/user",
  }
);
