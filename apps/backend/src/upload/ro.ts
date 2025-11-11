export class MediaUploadResponse {
  data: UploadData[];

  constructor(data: UploadData[]) {
    this.data = data;
  }
}

export class UploadData {
  url: string;
  key: string;
  type: string;
  isUploaded: boolean;
  message: string;

  /* eslint-disable max-params */

  constructor({
    url,
    key,
    type,
    isUploaded,
    message,
  }: {
    url: string;
    key: string;
    type: string;
    isUploaded: boolean;
    message: string;
  }) {
    this.url = url;
    this.key = key;
    this.type = type;
    this.isUploaded = isUploaded;
    this.message = message;
  }
}
