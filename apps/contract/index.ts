import { initContract } from "@ts-rest/core";
import { authContract } from "./auth/contract";
import { userContract } from "./user/contract";
import { jobContract } from "./job/contract";
import { analyticEventContract } from "./analyticEvent/contract";
import { subscriptionContract } from "./subscription/contract";
import { paidJobsContract } from "./paidJobs/contract";

const c = initContract();

export const contract = c.router({
  auth: authContract,
  user: userContract,
  job: jobContract,
  analyticEvent: analyticEventContract,
  subscription: subscriptionContract,
  paidJobs: paidJobsContract,
});
