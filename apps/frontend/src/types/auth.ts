import { ClientInferResponseBody } from "@ts-rest/core";
import { contract } from "contract";

export type GoogleUserDataType = ClientInferResponseBody<
  typeof contract.auth.getGoogleUser,
  200
>;
