import { usePageLoader } from "@/contexts/PageLoaderProvider";
import { logout } from "@/utils/common";
import { NotificationData, notifications } from "@mantine/notifications";
import flow from "lodash/flow";
import { ToastStatus, useCustomToast } from "@/hooks/useToast";
import { ZodError } from "zod";

export interface ErrorStatusInterface {
  message: string;
}
export interface MakeApiCallFunctionProps<T> {
  fetcherFn: () => Promise<T>;
  onSuccessFn?: (response: T) => void;
  onFailureFn?: (error: unknown) => void;
  successMsgProps?: NotificationData;
  failureMsgProps?: NotificationData;
  showLoader?: boolean;
  showFailureMsg?: boolean;
  finallyFn?: () => void;
}
const successToastId = "sucess-id";
const errorToastId = "errorToastId";

export const fetchDataWithThrowError = flow(fetch, async (res: any) => {
  const r = await res;
  const json = await r.json();
  if (!r.ok) {
    throw new Error(json.message);
  }
  return json;
});

export function useApi() {
  const { showPageLoader, hidePageLoader } = usePageLoader();
  const { showToast } = useCustomToast();
  const fetcherMakeApiCall = async ({
    fetcherFn,
    finallyFn,
  }: {
    fetcherFn: () => Promise<void>;
    finallyFn: () => void;
  }) => {
    try {
      await fetcherFn();
    } catch (error) {
      const errorResponse = error as ErrorStatusInterface;
      showToast({
        status: ToastStatus.error,
        id: errorToastId,
        message: errorResponse.message,
      });
    } finally {
      finallyFn();
    }
  };
  const makeApiCall = async function <
    T extends { status: number; body?: any }
  >({
    fetcherFn,
    onSuccessFn,
    onFailureFn,
    successMsgProps,
    failureMsgProps,
    showLoader = true,
    showFailureMsg = true,
    finallyFn,
  }: MakeApiCallFunctionProps<T>) {
    if (showLoader) {
      showPageLoader();
    }
    try {
      const response = await fetcherFn();
      if (response.status >= 200 && response.status <= 299) {
        hidePageLoader();
        if (successMsgProps) {
          notifications.clean();
          showToast({
            status: ToastStatus.success,
            id: successToastId,
            ...successMsgProps,
          });
        }
        if (onSuccessFn) {
          onSuccessFn(response);
        }
      } else if (response.status === 401) {
        logout();
      } else {
        if (response && response.body && response.body.message) {
          showToast({
            status: ToastStatus.error,
            id: errorToastId,
            message: response.body.message,
            ...failureMsgProps,
          });
        } else {
          const myError = new ZodError(response.body.issues);
          const obj = myError.flatten().fieldErrors;

          let outputString = "";
          for (const key in obj) {
            if (obj.hasOwnProperty(key)) {
              outputString += `${key} is ${(obj?.[key] ?? []).join(
                ", "
              )}. <br/>`;
            }
          }
          outputString = outputString.trim();

          showToast({
            status: ToastStatus.error,
            id: errorToastId,
            message: (
              <span dangerouslySetInnerHTML={{ __html: outputString }} />
            ),
            ...failureMsgProps,
          });
        }
        hidePageLoader();
      }
    } catch (error) {
      hidePageLoader();
      const errorResponse = error as ErrorStatusInterface;
      console.log("error");
      console.log(error);
      if (onFailureFn) {
        onFailureFn(error);
      }
      if (!showFailureMsg) {
        return;
      }
      showToast({
        status: ToastStatus.error,
        id: errorToastId,
        message: errorResponse.message,
        autoClose: 6000,
        ...failureMsgProps,
      });
    } finally {
      finallyFn && finallyFn();
    }
  };
  return { makeApiCall, fetcherMakeApiCall };
}
