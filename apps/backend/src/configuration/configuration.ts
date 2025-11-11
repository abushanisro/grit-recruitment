/* eslint-disable */
export default () => ({
  port: parseInt(process.env['PORT']!, 10) || 3000,
  database: {
    dbUrl: process.env['DATABASE_URL'],
  },
  google: {
    clientId: process.env['GOOGLE_CLIENT_ID'],
    clientSecret: process.env['GOOGLE_CLIENT_SECRET'],
    callbackURL: process.env['CALLBACK_URL'],
  },
  frontEndUrl: process.env['FRONTEND_URL'],
  adminFrontEndUrl: process.env['ADMIN_FRONTEND_URL'],
  aws: {
    accessKey: process.env['AWS_ACCESS_KEY'],
    secretKey: process.env['AWS_KEY_SECRET'],
    region: process.env['AWS_REGION'],
    bucket: process.env['AWS_S3_BUCKET'],
    pinPointApplicationId: process.env['PINPOINT_APPLICATION_ID'],
  },
  razorpay: {
    keyId: process.env['RAZORPAY_KEY_ID'],
    keySecret: process.env['RAZORPAY_KEY_SECRET'],
    webhookSecret: process.env['RAZORPAY_WEBHOOK_SECRET'],
  },
  slack: {
    botToken: process.env['SLACK_BOT_TOKEN'],
  },
  environment: process.env['ENV'],
});
