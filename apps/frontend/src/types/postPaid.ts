import { z } from "zod";
import { activeSubscriptionsResponse, subscriptionHistoryResponse } from "../../../contract/paidJobs/types";


export type PostPaidHistoryResponseType = z.infer<typeof subscriptionHistoryResponse >;
export type PostPaidActiveSubscriptionResponseType = z.infer<typeof activeSubscriptionsResponse  >;