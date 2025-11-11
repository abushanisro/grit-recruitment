import { Injectable } from '@nestjs/common';
import { Kysely } from 'kysely';
import { DB } from 'kysely-codegen';
import { InjectKysely } from 'nestjs-kysely';
import { CronExpressions, Schedule } from 'src/pgschedule/pgschedule.decorator';
import { WebClient } from '@slack/web-api';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class AlertService {
  private readonly web: WebClient;
  private readonly env: string;
  private readonly channelName = 'jobverse-logs';

  constructor(
    @InjectKysely() private readonly kdb: Kysely<DB>,
    private readonly configService: ConfigService,
  ) {
    const slackToken = this.configService.get<string>('slack.botToken')!;
    this.web = new WebClient(slackToken);
    this.env = this.configService.get<string>('environment')!;
  }

  delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

  @Schedule('errorMessagesAlert', CronExpressions.everyFiveMinutes)
  async emailAlerts() {
    if (this.env === 'dev') {
      return;
    }

    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);

    const failedJobs = await this.kdb
      .selectFrom('pgboss.job')
      .select(['id', 'output', 'name', 'completedon'])
      .where('state', '=', 'failed')
      .where('completedon', '>', fiveMinutesAgo)
      .whereRef('retrycount', '=', 'retrylimit')
      .execute();

    for (let index = 0; index < failedJobs.length; index++) {
      const job = failedJobs[index];
      if (
        job.output &&
        (job.output.toString().includes('ECONNRESET') ||
          job.output.toString().includes('ETIMEDOUT') ||
          job.output.toString().includes('BAD_REQUEST_ERROR')) &&
        this.env !== 'prod'
      ) {
        continue;
      }

      await this.delay(5 * 1000 * index);
      await this.web.chat.postMessage({
        channel: this.channelName,
        text: `Sales Jobverse ${this.env} Error :  ${job.name} with jobId : ${job.id} failed at ${job.completedon}`,
        blocks: [
          {
            type: 'header',
            text: {
              type: 'plain_text',
              text: `${this.env} Error :  ${job.name} with ${job.id} failed`,
            },
          },
          {
            type: 'section',
            text: {
              type: 'mrkdwn',
              text: `\`\`\`
                ${JSON.stringify(job.output).slice(0, 2800)}
              \`\`\``,
            },
          },
        ],
      });
    }
  }
}
