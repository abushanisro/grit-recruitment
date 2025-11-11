import { Migration } from '@mikro-orm/migrations';

export class Migration20240120093132 extends Migration {

  async up(): Promise<void> {
    this.addSql('alter table "job" drop constraint if exists "job_title_check";');

    this.addSql('alter table "job" drop constraint "job_company_id_foreign";');
    this.addSql('alter table "job" drop constraint "job_created_by_employer_user_id_foreign";');

    this.addSql('alter table "job" add column "is_external_job" boolean not null default false, add column "external_link" varchar(255) null default null, add column "external_job_company_name" varchar(255) null default null;');
    this.addSql('alter table "job" alter column "title" type varchar(150) using ("title"::varchar(150));');
    this.addSql('alter table "job" alter column "company_id" type int using ("company_id"::int);');
    this.addSql('alter table "job" alter column "company_id" drop not null;');
    this.addSql('alter table "job" alter column "description" type varchar(2000) using ("description"::varchar(2000));');
    this.addSql('alter table "job" alter column "description" drop not null;');
    this.addSql('alter table "job" alter column "created_by_employer_user_id" type int using ("created_by_employer_user_id"::int);');
    this.addSql('alter table "job" alter column "created_by_employer_user_id" drop not null;');
    this.addSql('alter table "job" add constraint "job_company_id_foreign" foreign key ("company_id") references "company" ("id") on update cascade on delete set null;');
    this.addSql('alter table "job" add constraint "job_created_by_employer_user_id_foreign" foreign key ("created_by_employer_user_id") references "employer" ("user_id") on update cascade on delete set null;');
  }

  async down(): Promise<void> {
    this.addSql('alter table "job" drop constraint "job_company_id_foreign";');
    this.addSql('alter table "job" drop constraint "job_created_by_employer_user_id_foreign";');

    this.addSql('alter table "job" alter column "title" type text using ("title"::text);');
    this.addSql('alter table "job" add constraint "job_title_check" check ("title" in (\'Software Engineer\', \'Team Lead\', \'Project Manager\', \'Product Manager\', \'Manager\', \'Marketing Manager\', \'Technical Support\', \'Senior Executive\', \'Quality Analyst\'));');
    this.addSql('alter table "job" alter column "company_id" type int using ("company_id"::int);');
    this.addSql('alter table "job" alter column "company_id" set not null;');
    this.addSql('alter table "job" alter column "description" type varchar(2000) using ("description"::varchar(2000));');
    this.addSql('alter table "job" alter column "description" set not null;');
    this.addSql('alter table "job" alter column "created_by_employer_user_id" type int using ("created_by_employer_user_id"::int);');
    this.addSql('alter table "job" alter column "created_by_employer_user_id" set not null;');
    this.addSql('alter table "job" add constraint "job_company_id_foreign" foreign key ("company_id") references "company" ("id") on update cascade;');
    this.addSql('alter table "job" add constraint "job_created_by_employer_user_id_foreign" foreign key ("created_by_employer_user_id") references "employer" ("user_id") on update cascade;');
  }

}
