import { notifications, NotificationData } from "@mantine/notifications";
import { Text } from "@mantine/core";
const toastId = "toast-id";
export enum ToastStatus {
  info = "info",
  warning = "warning",
  success = "success",
  error = "error",
}
export const getStyles = (status: ToastStatus) => {
  switch (status) {
    case ToastStatus.info:
      return {
        color: "blue",
      };
    case ToastStatus.warning:
      return {
        color: "orange",
      };
    case ToastStatus.success:
      return {
        color: "green",
      };
    case ToastStatus.error:
      return {
        color: "red",
      };
  }
};
export const useCustomToast = () => {
  const showToast = ({
    status = ToastStatus.success,
    ...props
  }: {
    status: ToastStatus;
  } & NotificationData) => {
    notifications.show({
      id: toastId,
      withCloseButton: true,
      autoClose: status === ToastStatus.success ? 3000 : 6000,
      loading: false,
      style: { color: "white" },
      bg: "var(--mantine-color-customBlack-1)",
      ...getStyles(status),
      ...props,
      message: (
        <Text c="white" style={{ userSelect: "none" }}>
          {props.message}
        </Text>
      ),
    });
  };

  return { showToast };
};
