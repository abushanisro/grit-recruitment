import { ClientInferResponseBody } from "@ts-rest/core";
import { contract } from "contract";
import { uploadSuccess } from "contract/upload/types";
import { infer as zodInfer } from "zod/lib/types";

export type UserProfileType = ClientInferResponseBody<
  typeof contract.user.getUserProfile,
  200
>;

export type UploadSuccessType = zodInfer<typeof uploadSuccess>;
