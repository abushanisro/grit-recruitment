import { Migration } from '@mikro-orm/migrations';

export class Migration20240322053312 extends Migration {

  async up(): Promise<void> {
    this.addSql('alter table "city" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "city" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "company" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "company" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');
    this.addSql('alter table "company" alter column "about_company" type text using ("about_company"::text);');
    this.addSql('alter table "company" alter column "about_company" set not null;');

    this.addSql('alter table "google_user" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "google_user" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "industry" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "industry" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "language" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "language" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "skill" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "skill" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "subfunction" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "subfunction" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "tools_and_software" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "tools_and_software" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "user" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "user" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "job_seeker" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "job_seeker" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "employer" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "employer" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "job" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "job" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');
    this.addSql('alter table "job" alter column "posted_time" type timestamptz using ("posted_time"::timestamptz);');
    this.addSql('alter table "job" alter column "admin_approved_time" type timestamptz using ("admin_approved_time"::timestamptz);');

    this.addSql('alter table "sent_job_alert" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "sent_job_alert" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "saved_job" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "saved_job" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "job_application" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "job_application" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');
    this.addSql('alter table "job_application" alter column "status_changed_time" type timestamptz using ("status_changed_time"::timestamptz);');

    this.addSql('alter table "email_invite" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "email_invite" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');

    this.addSql('alter table "analytic_event" alter column "created_at" type timestamptz using ("created_at"::timestamptz);');
    this.addSql('alter table "analytic_event" alter column "updated_at" type timestamptz using ("updated_at"::timestamptz);');
  }

  async down(): Promise<void> {
    this.addSql('alter table "city" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "city" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "company" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "company" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');
    this.addSql('alter table "company" alter column "about_company" type text using ("about_company"::text);');
    this.addSql('alter table "company" alter column "about_company" drop not null;');

    this.addSql('alter table "google_user" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "google_user" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "industry" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "industry" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "language" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "language" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "skill" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "skill" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "subfunction" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "subfunction" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "tools_and_software" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "tools_and_software" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "user" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "user" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "job_seeker" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "job_seeker" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "employer" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "employer" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "job" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "job" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');
    this.addSql('alter table "job" alter column "posted_time" type timestamptz(0) using ("posted_time"::timestamptz(0));');
    this.addSql('alter table "job" alter column "admin_approved_time" type timestamptz(0) using ("admin_approved_time"::timestamptz(0));');

    this.addSql('alter table "sent_job_alert" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "sent_job_alert" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "saved_job" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "saved_job" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "job_application" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "job_application" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');
    this.addSql('alter table "job_application" alter column "status_changed_time" type timestamptz(0) using ("status_changed_time"::timestamptz(0));');

    this.addSql('alter table "email_invite" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "email_invite" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');

    this.addSql('alter table "analytic_event" alter column "created_at" type timestamptz(0) using ("created_at"::timestamptz(0));');
    this.addSql('alter table "analytic_event" alter column "updated_at" type timestamptz(0) using ("updated_at"::timestamptz(0));');
  }

}
