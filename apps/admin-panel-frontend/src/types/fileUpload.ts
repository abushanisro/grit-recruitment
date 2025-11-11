import { FileUploadErrorResponseSchema } from "contract/common";
import { z } from "zod";

export interface FileUploadFormInterface {
  file: File | null;
}

export type FileErrorType = z.infer<typeof FileUploadErrorResponseSchema>;
