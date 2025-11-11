/** @type {import('next').NextConfig} */

const getEnvConfig = () => {
  switch (process.env.ENV) {
    case "dev":
      return {
        env: "dev",
        apiUrl: "http://localhost:3000",
        frontendUrl: "http://localhost:3002",
        projectId: "sales-jobverse-dev",
        s3BucketBaseUrl:
          "https://jobverse-sales-stage-media.s3.ap-south-2.amazonaws.com",
      };
    case "stage":
      return {
        env: "stage",
        apiUrl: "https://stage-backend.salesjobverse.com",
        frontendUrl: "https://admin-stage.salesjobverse.com",
        projectId: "sales-jobverse-admin-stage",
        s3BucketBaseUrl:
          "https://jobverse-sales-stage-media.s3.ap-south-2.amazonaws.com",
      };
    case "prod":
      return {
        env: "prod",
        apiUrl: "https://prod-backend.salesjobverse.com",
        frontendUrl: "https://admin-prod.salesjobverse.com",
        projectId: "sales-jobverse-admin-prod",
        s3BucketBaseUrl:
          "https://jobverse-sales-prod-media-media.s3.ap-south-2.amazonaws.com",
      };
  }
};
const nextConfig = {
  output: "export",
  reactStrictMode: true,
  publicRuntimeConfig: getEnvConfig(),
  trailingSlash: true,
  experimental: {
    externalDir: true,
  },
};

module.exports = nextConfig;
