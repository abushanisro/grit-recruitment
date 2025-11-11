import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as AWS from 'aws-sdk';
import { mapValues } from 'lodash';

@Injectable()
export class EmailService {
  constructor(private configService: ConfigService) {}

  pinpoint = new AWS.Pinpoint({
    accessKeyId: this.configService.get<string>('aws.accessKey') as string,
    secretAccessKey: this.configService.get<string>('aws.secretKey') as string,
    region: this.configService.get<string>('aws.region') as string,
  });

  sendEmailMsg(params: AWS.Pinpoint.SendMessagesRequest) {
    return new Promise((resolve, reject) => {
      this.pinpoint.sendMessages(params, (err: AWS.AWSError | null, data) => {
        if (err) {
          console.log(`Error send email message: ${err.message}`);
          reject(err);
        } else {
          console.log(
            `Email successfully sent - ${JSON.stringify(data, null, 2)}`,
          );
          resolve(data);
        }
      });
    });
  }

  public async sendEmail(msg: string, name: string, recipientEmail: string) {
    const SES_CONFIG = {
      accessKeyId: this.configService.get<string>('aws.accessKey') as string,
      secretAccessKey: this.configService.get<string>(
        'aws.secretKey',
      ) as string,
      region: this.configService.get<string>('aws.region') as string,
    };

    const AWS_SES = new AWS.SES(SES_CONFIG);
    const sendAddress = 'admin@marketingjobverse.com';

    const params = {
      Source: sendAddress,
      Destination: {
        ToAddresses: [recipientEmail],
      },
      ReplyToAddresses: [],
      Message: {
        Body: {
          Html: {
            Charset: 'UTF-8',
            Data: msg,
          },
        },
        Subject: {
          Charset: 'UTF-8',
          Data: `Hello, ${name}!`,
        },
      },
    };

    return AWS_SES.sendEmail(params).promise();
  }

  sendAddress = 'admin@salesjobverse.com';

  async sendUserEmail(
    templateParams: Record<string, string>,
    to: string,
    template: string,
  ) {
    await this.sendEmailMsg({
      ApplicationId: '8118da6cdf7349ab8066ccebd404c272',
      MessageRequest: {
        Addresses: {
          [to]: {
            ChannelType: 'EMAIL',
          },
        },
        MessageConfiguration: {
          EmailMessage: {
            FromAddress: this.sendAddress,
            Substitutions: mapValues(templateParams, (v) => [v]),
          },
        },
        TemplateConfiguration: {
          EmailTemplate: {
            Name: template,
          },
        },
      },
    });
  }

  async sendJobAlertEmail(
    templateParams: JobAlertEmailTemplateParams,
    to: string,
  ) {
    await this.sendEmailMsg({
      ApplicationId: this.configService.get<string>(
        'aws.pinPointApplicationId',
      )!,
      MessageRequest: {
        Addresses: {
          [to]: {
            ChannelType: 'EMAIL',
          },
        },
        MessageConfiguration: {
          EmailMessage: {
            FromAddress: this.sendAddress,
            Substitutions: mapValues(templateParams, (v) => [v]),
          },
        },
        TemplateConfiguration: {
          EmailTemplate: {
            Name: 'jobverse-alert-stage',
          },
        },
      },
    });
  }
}
interface JobAlertEmailTemplateParams {
  userName: string;
  jobTitle: string;
  companyName: string;
  locationName: string;
  jobType: string;
  description: string;
  jobLink: string;
}
