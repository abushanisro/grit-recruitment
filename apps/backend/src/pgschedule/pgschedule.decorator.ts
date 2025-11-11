import { SetMetadata } from '@nestjs/common';
import { QueueNamesEnum, QueuePayloadData } from './config/queueConfig';

export const SCHEDULE_META = Symbol.for('SCHEDULE_META');
export const QUEUE_META = Symbol.for('QUEUE_META');

// eslint-disable-next-line @typescript-eslint/naming-convention
export const Schedule = (name: string, cron: string) =>
  SetMetadata(SCHEDULE_META, { name, cron });

export const QueueMessageHandler = <T extends QueueNamesEnum>(
  name: T,
): ((
  target: object,
  propertyKey: string | symbol,
  descriptor: TypedPropertyDescriptor<
    (payload: QueuePayloadData<T>) => Promise<void>
  >,
) => TypedPropertyDescriptor<
  (payload: QueuePayloadData<T>) => Promise<void>
>) => SetMetadata(QUEUE_META, { name });

export enum CronExpressions {
  everyMinute = '* * * * *',
  everyFiveMinutes = '*/5 * * * *',
  everyTenMinutes = '*/10 * * * *',
  everyFifteenMinutes = '*/15 * * * *',
  everyThirtyMinutes = '*/30 * * * *',
  everyHour = '0 * * * *',
  everyTwoHours = '0 */2 * * *',
  everyThreeHours = '0 */3 * * *',
  everyFourHours = '0 */4 * * *',
  everySixHours = '0 */6 * * *',
  everyTwelveHours = '0 */12 * * *',
  everyDay = '0 0 * * *',
  dailyAlertTime = '0 4 * * *',
}
