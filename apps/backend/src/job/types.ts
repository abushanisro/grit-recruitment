import { BulkUploadJobsForJobSeekerDto } from 'contract/job/types';

export type BulkUploadJobSeekerJobsSuccessType = {
  kind: 'success';
  data: BulkUploadJobsForJobSeekerDto;
  errors: string[];
};

export type BulkUploadJobSeekerJobsErrorType = {
  kind: 'error';
  data: Record<string, unknown>;
  errors: string[];
};
