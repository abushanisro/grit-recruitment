const publicRuntimeConfig =
  require("next/config").default().publicRuntimeConfig;

const { apiUrl, env, projectId, frontendUrl } = publicRuntimeConfig;

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
const instagramLink =
  "https://instagram.com/salesjobverse?igshid=NGVhN2U2NjQ0Yg==";

export { getEnv, getApiUrl, getProjectId, getFrontendUrl, instagramLink };
