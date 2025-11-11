/** @type {import('next').NextConfig} */

const getEnvConfig = () => {
  switch (process.env.ENV) {
    case "dev":
      return {
        env: "dev",
        apiUrl: "http://localhost:3000",
        frontendUrl: "http://localhost:3001",
        projectId: "sales-jobverse-dev",
      };
    case "stage":
      return {
        env: "stage",
        apiUrl: "https://stage-backend.salesjobverse.com",
        frontendUrl: "https://stage.salesjobverse.com",
        projectId: "sales-jobverse-stage",
      };
    case "prod":
      return {
        env: "prod",
        apiUrl: "https://prod-backend.salesjobverse.com",
        frontendUrl: "https://www.salesjobverse.com",
        projectId: "sales-jobverse-prod",
      };
  }
};
const nextConfig = {
  output: "export", // reenable if deploying to s3
  reactStrictMode: true,
  publicRuntimeConfig: getEnvConfig(),
  trailingSlash: true,
  experimental: {
    externalDir: true,
  },
};

const withBundleAnalyzer = require("@next/bundle-analyzer")({
  enabled: process.env.ANALYZE === "true",
});

module.exports = withBundleAnalyzer(nextConfig);
