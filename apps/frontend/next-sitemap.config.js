/** @type {import('next-sitemap').IConfig} */

module.exports = {
  siteUrl: process.env.FRONTEND_URL,
  generateRobotsTxt: true,
  robotsTxtOptions: {
    policies: [
      {
        userAgent: "*",
        allow: "/",
      },
    ],
  },
};
