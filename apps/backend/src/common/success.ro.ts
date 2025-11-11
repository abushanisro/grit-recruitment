export class SuccessRO {
  isSuccess = true;
  message = 'Success';

  constructor(message = 'Success') {
    this.isSuccess = true;
    this.message = message;
  }
}

export const getSuccessMsg = (msg?: string) => ({
  status: 200 as const,
  body: { isSuccess: true, message: msg ?? 'Success' },
});
