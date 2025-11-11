const publicRuntimeConfig =
  require("next/config").default().publicRuntimeConfig;

const { apiUrl, env, projectId, frontendUrl, s3BucketBaseUrl } =
  publicRuntimeConfig;

const getEnv = () => {
  return env;
};

const getApiUrl = () => {
  return apiUrl;
};

const getProjectId = () => {
  return projectId;
};

const getFrontendUrl = () => {
  return frontendUrl;
};
const getS3BucketBaseUrl = () => {
  return s3BucketBaseUrl;
};
export { getEnv, getApiUrl, getProjectId, getFrontendUrl, getS3BucketBaseUrl };
