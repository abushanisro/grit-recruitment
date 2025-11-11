import { Migration } from '@mikro-orm/migrations';

export class Migration20240110074322 extends Migration {

  async up(): Promise<void> {
    this.addSql('create table "city" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "name" varchar(255) not null);');

    this.addSql('create table "company" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "name" varchar(255) not null, "website" text null, "about_company" text null, "company_size" text check ("company_size" in (\'Selfemployed\', \'1-30\', \'30-50\', \'50-200\', \'200-500\', \'500-600\', \'600-800\', \'800-1000\', \'1000-3000\', \'3000-5000\', \'5000-7000\', \'7000-10000\', \'10000 >\')) null, "logo" text null);');

    this.addSql('create table "google_user" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "first_name" varchar(255) not null, "last_name" varchar(255) not null, "email" varchar(255) not null, "picture" varchar not null, "reference_id" varchar(255) not null);');
    this.addSql('alter table "google_user" add constraint "google_user_reference_id_unique" unique ("reference_id");');

    this.addSql('create table "industry" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "name" varchar(255) not null);');

    this.addSql('create table "company_industries" ("company_id" int not null, "industry_id" int not null, constraint "company_industries_pkey" primary key ("company_id", "industry_id"));');

    this.addSql('create table "language" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "name" varchar(255) not null);');

    this.addSql('create table "skill" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "name" varchar(255) not null);');

    this.addSql('create table "subfunction" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "name" varchar(255) not null);');

    this.addSql('create table "tools_and_software" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "name" varchar(255) not null);');

    this.addSql('create table "user" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "first_name" varchar(255) not null, "last_name" varchar(255) not null, "email" varchar(255) not null, "phone" varchar(255) not null, "role" text check ("role" in (\'jobSeeker\', \'admin\', \'employer\')) not null, "picture" text null);');
    this.addSql('alter table "user" add constraint "user_email_unique" unique ("email");');

    this.addSql('create table "job_seeker" ("user_id" int not null, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "profile_summary" text not null, "headline" varchar(255) not null, "expected_salary_in_lpa" int null, "is_private" boolean not null, "external_link" text null, "resume" text not null, "video_resume" text not null, "experience_in_year" double precision not null, "notice_period" text check ("notice_period" in (\'immediately\', \'oneWeek\', \'twoWeeks\', \'threeWeeks\', \'oneMonth\', \'twoMonths\', \'threeMonths\', \'fourMonths\', \'fiveMonths\', \'sixMonths\', \'moreThanSixMonths\')) not null, "work_schedule" text check ("work_schedule" in (\'Remote\', \'Hybrid\', \'Onsite\')) null, "city_id" int not null, "social_media_link" text null, "is_subscribed_to_alerts" boolean not null, constraint "job_seeker_pkey" primary key ("user_id"));');

    this.addSql('create table "job_seeker_subfunction" ("job_seeker_user_id" int not null, "subfunction_id" int not null, constraint "job_seeker_subfunction_pkey" primary key ("job_seeker_user_id", "subfunction_id"));');

    this.addSql('create table "job_seeker_skills" ("job_seeker_user_id" int not null, "skill_id" int not null, constraint "job_seeker_skills_pkey" primary key ("job_seeker_user_id", "skill_id"));');

    this.addSql('create table "job_seeker_preferred_locations" ("job_seeker_user_id" int not null, "city_id" int not null, constraint "job_seeker_preferred_locations_pkey" primary key ("job_seeker_user_id", "city_id"));');

    this.addSql('create table "job_seeker_languages" ("job_seeker_user_id" int not null, "language_id" int not null, constraint "job_seeker_languages_pkey" primary key ("job_seeker_user_id", "language_id"));');

    this.addSql('create table "employer" ("user_id" int not null, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "company_id" int not null, "verification_document" text not null, "is_verified" boolean not null, "city_id" int null, constraint "employer_pkey" primary key ("user_id"));');
    this.addSql('alter table "employer" add constraint "employer_user_id_unique" unique ("user_id");');

    this.addSql('create table "job" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "title" text check ("title" in (\'Software Engineer\', \'Team Lead\', \'Project Manager\', \'Product Manager\', \'Manager\', \'Marketing Manager\', \'Technical Support\', \'Senior Executive\', \'Quality Analyst\')) not null, "company_id" int not null, "description" varchar(2000) not null, "min_salary_in_lpa" numeric null default null, "max_salary_in_lpa" numeric null default null, "employment_types" text[] not null, "employment_modes" text[] not null, "experience_in_years" int not null, "created_by_employer_user_id" int not null, "is_deleted" boolean not null default false, "is_posted" boolean not null, "posted_time" timestamptz(0) null default null, "search_vector" text null default null, "is_admin_approved" boolean not null default false, "admin_approved_time" timestamptz(0) null default null, constraint admin_approve_check check (not ((is_admin_approved=true) and (admin_approved_time is null))));');

    this.addSql('create table "sent_job_alert" ("job_id" int not null, "user_id" int not null, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, constraint "sent_job_alert_pkey" primary key ("job_id", "user_id"));');

    this.addSql('create table "saved_job" ("user_id" int not null, "job_id" int not null, "is_deleted" boolean not null default false, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, constraint "saved_job_pkey" primary key ("user_id", "job_id"));');

    this.addSql('create table "job_application" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "job_seeker_user_id" int not null, "job_id" int not null, "status" text check ("status" in (\'Pending\', \'Shortlisted\', \'Rejected\')) not null, "status_changed_time" timestamptz(0) null default null, "cover_letter" varchar(2000) null default null, "are_you_okay_with_the_location" boolean not null);');
    this.addSql('alter table "job_application" add constraint "job_application_job_seeker_user_id_job_id_unique" unique ("job_seeker_user_id", "job_id");');

    this.addSql('create table "job_sub_functions" ("job_id" int not null, "subfunction_id" int not null, constraint "job_sub_functions_pkey" primary key ("job_id", "subfunction_id"));');

    this.addSql('create table "job_industries" ("job_id" int not null, "industry_id" int not null, constraint "job_industries_pkey" primary key ("job_id", "industry_id"));');

    this.addSql('create table "job_cities" ("job_id" int not null, "city_id" int not null, constraint "job_cities_pkey" primary key ("job_id", "city_id"));');

    this.addSql('create table "email_invite" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "user_id" int not null, "ref_id" varchar(255) not null, "is_used" boolean not null);');

    this.addSql('create table "analytic_event" ("id" serial primary key, "created_at" timestamptz(0) not null, "updated_at" timestamptz(0) not null, "user_id" int not null, "event_name" varchar(255) not null, "page_url" text not null, "device" varchar(255) not null, "browser" varchar(255) not null, "user_agent" varchar(255) not null);');

    this.addSql('alter table "company_industries" add constraint "company_industries_company_id_foreign" foreign key ("company_id") references "company" ("id") on update cascade on delete cascade;');
    this.addSql('alter table "company_industries" add constraint "company_industries_industry_id_foreign" foreign key ("industry_id") references "industry" ("id") on update cascade on delete cascade;');

    this.addSql('alter table "job_seeker" add constraint "job_seeker_user_id_foreign" foreign key ("user_id") references "user" ("id") on update cascade on delete cascade;');
    this.addSql('alter table "job_seeker" add constraint "job_seeker_city_id_foreign" foreign key ("city_id") references "city" ("id") on update cascade;');

    this.addSql('alter table "job_seeker_subfunction" add constraint "job_seeker_subfunction_job_seeker_user_id_foreign" foreign key ("job_seeker_user_id") references "job_seeker" ("user_id") on update cascade on delete cascade;');
    this.addSql('alter table "job_seeker_subfunction" add constraint "job_seeker_subfunction_subfunction_id_foreign" foreign key ("subfunction_id") references "subfunction" ("id") on update cascade on delete cascade;');

    this.addSql('alter table "job_seeker_skills" add constraint "job_seeker_skills_job_seeker_user_id_foreign" foreign key ("job_seeker_user_id") references "job_seeker" ("user_id") on update cascade on delete cascade;');
    this.addSql('alter table "job_seeker_skills" add constraint "job_seeker_skills_skill_id_foreign" foreign key ("skill_id") references "skill" ("id") on update cascade on delete cascade;');

    this.addSql('alter table "job_seeker_preferred_locations" add constraint "job_seeker_preferred_locations_job_seeker_user_id_foreign" foreign key ("job_seeker_user_id") references "job_seeker" ("user_id") on update cascade on delete cascade;');
    this.addSql('alter table "job_seeker_preferred_locations" add constraint "job_seeker_preferred_locations_city_id_foreign" foreign key ("city_id") references "city" ("id") on update cascade on delete cascade;');

    this.addSql('alter table "job_seeker_languages" add constraint "job_seeker_languages_job_seeker_user_id_foreign" foreign key ("job_seeker_user_id") references "job_seeker" ("user_id") on update cascade on delete cascade;');
    this.addSql('alter table "job_seeker_languages" add constraint "job_seeker_languages_language_id_foreign" foreign key ("language_id") references "language" ("id") on update cascade on delete cascade;');

    this.addSql('alter table "employer" add constraint "employer_user_id_foreign" foreign key ("user_id") references "user" ("id") on update cascade on delete cascade;');
    this.addSql('alter table "employer" add constraint "employer_company_id_foreign" foreign key ("company_id") references "company" ("id") on update cascade;');
    this.addSql('alter table "employer" add constraint "employer_city_id_foreign" foreign key ("city_id") references "city" ("id") on update cascade on delete set null;');

    this.addSql('alter table "job" add constraint "job_company_id_foreign" foreign key ("company_id") references "company" ("id") on update cascade;');
    this.addSql('alter table "job" add constraint "job_created_by_employer_user_id_foreign" foreign key ("created_by_employer_user_id") references "employer" ("user_id") on update cascade;');

    this.addSql('alter table "sent_job_alert" add constraint "sent_job_alert_job_id_foreign" foreign key ("job_id") references "job" ("id") on update cascade;');
    this.addSql('alter table "sent_job_alert" add constraint "sent_job_alert_user_id_foreign" foreign key ("user_id") references "user" ("id") on update cascade;');

    this.addSql('alter table "saved_job" add constraint "saved_job_user_id_foreign" foreign key ("user_id") references "user" ("id") on update cascade;');
    this.addSql('alter table "saved_job" add constraint "saved_job_job_id_foreign" foreign key ("job_id") references "job" ("id") on update cascade;');

    this.addSql('alter table "job_application" add constraint "job_application_job_seeker_user_id_foreign" foreign key ("job_seeker_user_id") references "job_seeker" ("user_id") on update cascade;');
    this.addSql('alter table "job_application" add constraint "job_application_job_id_foreign" foreign key ("job_id") references "job" ("id") on update cascade;');

    this.addSql('alter table "job_sub_functions" add constraint "job_sub_functions_job_id_foreign" foreign key ("job_id") references "job" ("id") on update cascade on delete cascade;');
    this.addSql('alter table "job_sub_functions" add constraint "job_sub_functions_subfunction_id_foreign" foreign key ("subfunction_id") references "subfunction" ("id") on update cascade on delete cascade;');

    this.addSql('alter table "job_industries" add constraint "job_industries_job_id_foreign" foreign key ("job_id") references "job" ("id") on update cascade on delete cascade;');
    this.addSql('alter table "job_industries" add constraint "job_industries_industry_id_foreign" foreign key ("industry_id") references "industry" ("id") on update cascade on delete cascade;');

    this.addSql('alter table "job_cities" add constraint "job_cities_job_id_foreign" foreign key ("job_id") references "job" ("id") on update cascade on delete cascade;');
    this.addSql('alter table "job_cities" add constraint "job_cities_city_id_foreign" foreign key ("city_id") references "city" ("id") on update cascade on delete cascade;');

    this.addSql('alter table "email_invite" add constraint "email_invite_user_id_foreign" foreign key ("user_id") references "user" ("id") on update cascade;');

    this.addSql('alter table "analytic_event" add constraint "analytic_event_user_id_foreign" foreign key ("user_id") references "user" ("id") on update cascade;');
  }

}
