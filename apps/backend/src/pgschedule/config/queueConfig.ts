// For adding new Queue:
//  1. Add queue name in QueueNamesEnum
//  2. Add queue -> payload mapping in QueuePayloadDataMap

// Replace with your actual queue names
export enum QueueNamesEnum {
  jobInvite = 'jobInvite',
  boostJobEmail = 'boostJobEmail',
}

interface QueuePayloadDataMap {
  [QueueNamesEnum.jobInvite]: { inviteId: string };
  [QueueNamesEnum.boostJobEmail]: { jobId: string };
}

export type QueuePayloadData<T extends QueueNamesEnum> = {
  name: T;
  data: QueuePayloadDataMap[T];
};
