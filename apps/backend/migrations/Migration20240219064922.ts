import { Migration } from '@mikro-orm/migrations';

export class Migration20240219064922 extends Migration {

  async up(): Promise<void> {
    this.addSql('alter table "job_seeker" drop constraint if exists "job_seeker_notice_period_check";');

    this.addSql('alter table "job_seeker" alter column "notice_period" type text using ("notice_period"::text);');
    this.addSql('alter table "job_seeker" add constraint "job_seeker_notice_period_check" check ("notice_period" in (\'immediately\', \'twoWeeks\', \'oneMonth\', \'sixMonths\', \'moreThanSixMonths\'));');
  }

  async down(): Promise<void> {
    this.addSql('alter table "job_seeker" drop constraint if exists "job_seeker_notice_period_check";');

    this.addSql('alter table "job_seeker" alter column "notice_period" type text using ("notice_period"::text);');
    this.addSql('alter table "job_seeker" add constraint "job_seeker_notice_period_check" check ("notice_period" in (\'immediately\', \'oneWeek\', \'twoWeeks\', \'threeWeeks\', \'oneMonth\', \'twoMonths\', \'threeMonths\', \'fourMonths\', \'fiveMonths\', \'sixMonths\', \'moreThanSixMonths\'));');
  }

}
