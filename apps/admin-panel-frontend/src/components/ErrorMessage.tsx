import { Center, Text } from "@mantine/core";
import { ErrorHttpStatusCode } from "@ts-rest/core";

interface ErrorBody {
  message?: string;
}
export interface ErrorMessageInterface {
  status: ErrorHttpStatusCode;
  body: ErrorBody | unknown;
  headers: Headers;
}

export const getErrorMessage = (error: {
  status: number;
  body: unknown;
}): string => {
  const isErrorResponse = (object: any): object is ErrorBody => {
    return typeof object === "object" && object !== null && "message" in object;
  };
  if (error.status === 404 && isErrorResponse(error.body)) {
    return error.body.message ?? "Not found";
  }
  if (isErrorResponse(error.body)) {
    return error.body.message ?? "An error occurred";
  }
  return "An error occurred";
};

const ErrorMessage = ({ error }: { error: ErrorMessageInterface }) => {
  return (
    <Center bg="customBlack.4" h="100vh">
      <Text c="white">{getErrorMessage(error)}</Text>
    </Center>
  );
};

export default ErrorMessage;
