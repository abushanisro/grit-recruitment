import { initContract } from "@ts-rest/core";
import {
  createPaginatedResponseSchema,
  createPaginatedResponseSchemaForManageApplicantCount,
} from "../utils";
import {
  addJobDto,
  changeApplicationStatusDto,
  commonInfo,
  errorMessageResponse,
  fetchAppliedJobResponse,
  paginationDto,
  fetchJobsResponse,
  jobFilterResponse,
  jobIdResponse,
  jobSearchDto,
  jobSeekerJobsResponse,
  jobSeekerProfileResponse,
  jobViewDto,
  applyJobDto,
  manageJobApplicationsOfJobseekersResponse,
  manageJobApplicationsOfJobseekersDto,
  getSuggestionJobTitleInManageApplicantsJobseekerResponse,
  fetchJobsByEmployerJobSeekerDto,
} from "./types";
import { z } from "zod";
import { FileUploadErrorResponseSchema, SuccessSchema } from "contract/common";

const c = initContract();

export const jobContract = c.router(
  {
    filter: {
      method: "GET",
      path: "/searchandfilter",
      responses: {
        200: createPaginatedResponseSchema(jobFilterResponse),
      },
      query: jobSearchDto,
    },

    getJobById: {
      method: "GET",
      path: "/viewjobasjobseeker",
      responses: {
        200: jobIdResponse,
        404: errorMessageResponse,
      },
      query: jobViewDto,
    },

    getSuggestionLocation: {
      method: "GET",
      path: "/suggestionLocation",
      responses: {
        200: commonInfo.array(),
      },
      query: z.object({
        searchText: z.string().optional(),
      }),
    },

    getSuggestionSkill: {
      method: "GET",
      path: "/suggestionSkill",
      responses: {
        200: commonInfo.array(),
      },
      query: z.object({
        searchText: z.string().optional(),
      }),
    },

    getSuggestionIndustry: {
      method: "GET",
      path: "/suggestionIndustry",
      responses: {
        200: commonInfo.array(),
      },
      query: z.object({
        searchText: z.string().optional(),
      }),
    },

    getSuggestionSubFunction: {
      method: "GET",
      path: "/suggestionSubFunction",
      responses: {
        200: commonInfo.array(),
      },
      query: z.object({
        searchText: z.string().optional(),
      }),
    },
    getSuggestionLanguages: {
      method: "GET",
      path: "/suggestionLanguages",
      responses: {
        200: commonInfo.array(),
      },
      query: z.object({
        searchText: z.string().optional(),
      }),
    },
    getSuggestionToolsAndSoftware: {
      method: "GET",
      path: "/suggestionToolsAndSoftware",
      responses: {
        200: commonInfo.array(),
      },
      query: z.object({
        searchText: z.string().optional(),
      }),
    },

    applyJob: {
      method: "POST",
      path: "/applyJob",
      responses: {
        200: SuccessSchema,
      },
      body: applyJobDto,
    },

    getAppliedJobs: {
      method: "GET",
      path: "/fetchAppliedJobs",
      responses: {
        200: createPaginatedResponseSchema(fetchAppliedJobResponse),
      },
      query: paginationDto,
    },

    saveJob: {
      method: "POST",
      path: "/saveJob",
      responses: {
        200: SuccessSchema,
      },
      body: z.object({
        jobId: z.number(),
      }),
    },

    unsaveJob: {
      method: "PUT",
      path: "/unSaveJobs",
      responses: {
        200: SuccessSchema,
      },
      body: z.object({ jobId: z.number() }),
    },

    fetchSavedJobs: {
      method: "GET",
      path: "/fetchSavedJobs",
      responses: {
        200: createPaginatedResponseSchema(jobSeekerJobsResponse),
      },
      query: paginationDto,
    },

    fetchJobsByEmployer: {
      method: "GET",
      path: "/fetchJobsAsEmployer",
      responses: {
        200: createPaginatedResponseSchema(fetchJobsResponse),
      },
      query: fetchJobsByEmployerJobSeekerDto,
    },

    createJobForJobSeeker: {
      method: "POST",
      path: "/addJobAsEmployer",
      responses: {
        200: SuccessSchema,
      },
      body: addJobDto,
    },

    getCompanyNameSuggestions: {
      method: "GET",
      path: "/suggestionCompanyName",
      responses: {
        200: commonInfo.array(),
      },
      query: z.object({
        searchText: z.string().optional(),
      }),
    },

    viewJobSeekersProfile: {
      method: "GET",
      path: "/viewJobSeekersProfile",
      responses: {
        200: jobSeekerProfileResponse,
      },
      query: z.object({
        userId: z.string().transform(Number),
      }),
    },

    deleteJobAsEmployer: {
      method: "DELETE",
      path: "/deleteJobAsEmployer",
      responses: {
        200: SuccessSchema,
      },
      body: z.object({ jobId: z.number() }),
    },

    changeJobSeekerApplicationStatus: {
      method: "PUT",
      path: "/changeJobSeekerApplicationStatus",
      responses: {
        200: SuccessSchema,
      },
      body: changeApplicationStatusDto,
    },

    changeIsPost: {
      method: "PUT",
      path: "/changeIsPost",
      responses: {
        200: SuccessSchema,
      },
      body: z.object({
        jobId: z.number(),
      }),
    },

    manageJobApplicationsOfJobseekers: {
      method: "GET",
      path: "/manageJobApplicationsOfJobseekers",
      responses: {
        200: createPaginatedResponseSchemaForManageApplicantCount(
          manageJobApplicationsOfJobseekersResponse
        ),
      },
      query: manageJobApplicationsOfJobseekersDto,
    },

    getSuggestionJobTitleInManageApplicantsJobseeker: {
      method: "GET",
      path: "/getSuggestionJobTitleInManageApplicantsJobseeker",
      responses: {
        200: getSuggestionJobTitleInManageApplicantsJobseekerResponse,
      },
      query: z.object({
        searchText: z.string().optional(),
      }),
    },
    bulkUploadJobsForJobSeeker: {
      method: "POST",
      path: "/bulkUploadJobsForJobSeeker",
      contentType: "multipart/form-data",
      responses: {
        200: z.discriminatedUnion("kind", [
          SuccessSchema.extend({ kind: z.literal("success") }),
          FileUploadErrorResponseSchema,
        ]),
      },
      body: c.type<{ file: Express.Multer.File }>(),
    },

    deleteJobseekerJobAsAdmin: {
      method: "DELETE",
      path: "/deleteJobseekerJobAsAdmin",
      responses: {
        200: SuccessSchema,
      },
      body: z.object({ jobId: z.number() }),
    },
  },
  {
    pathPrefix: "/job",
  }
);
