import { getApiUrl } from "@/env";
export const uploadFiles = (files: File[], contentDisposition?: string) => {
  const formData = new FormData();

  files.forEach((f) => {
    formData.append("files", f);
  });

  const url = contentDisposition
    ? `/upload/media?contentDisposition=${contentDisposition}`
    : "/upload/media";

  return fetch(`${getApiUrl()}${url}`, {
    method: "POST",
    body: formData,
  });
};
