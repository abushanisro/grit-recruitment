import { Migration } from '@mikro-orm/migrations';

export class Migration20240621090648 extends Migration {

  async up(): Promise<void> {
    this.addSql('create table "job_seeker_subscription_plan" ("id" varchar(255) not null, "price" int not null, "name" varchar(255) not null, "valid_for_days" int not null, "description" text not null, "allowed_profile_count" int not null, constraint "job_seeker_subscription_plan_pkey" primary key ("id"));');

    this.addSql('create table "paid_job_subscription_plan" ("id" varchar(255) not null, "price" int not null, "name" varchar(255) not null, "valid_for_days" int not null, "boost_limit" int not null, "boost_days" int not null, "description" text not null, "points" int not null, constraint "paid_job_subscription_plan_pkey" primary key ("id"));');

    this.addSql('create table "paid_job_subscription_order" ("id" serial primary key, "created_at" timestamptz not null, "updated_at" timestamptz not null, "valid_for_days" int not null, "employer_user_id" int not null, "payment_method" varchar(255) not null, "payment_status" text check ("payment_status" in (\'success\', \'failure\', \'pending\')) not null, "paid_amount" int not null, "razorpay_order_id" varchar(255) not null, "razorpay_payment_id" varchar(255) null, "razorpay_invoice_id" varchar(255) not null, "invoice_link" varchar(255) null, "plan_id" varchar(255) not null, "plan_name" varchar(255) not null, "boost_limit" int not null, "boost_days" int not null);');

    this.addSql('create table "job_seeker_subscription_order" ("id" serial primary key, "created_at" timestamptz not null, "updated_at" timestamptz not null, "valid_for_days" int not null, "employer_user_id" int not null, "payment_method" varchar(255) not null, "payment_status" text check ("payment_status" in (\'success\', \'failure\', \'pending\')) not null, "paid_amount" int not null, "razorpay_order_id" varchar(255) not null, "razorpay_payment_id" varchar(255) null, "razorpay_invoice_id" varchar(255) not null, "invoice_link" varchar(255) null, "allowed_profile_count" int not null, "plan_id" varchar(255) not null);');

    this.addSql('create table "employer_paid_job_subscription" ("id" serial primary key, "employer_user_id" int not null, "valid_for_days" int not null, "created_at" timestamptz not null, "updated_at" timestamptz not null, "expiry_date" timestamptz not null, "plan_name" varchar(255) not null, "boost_limit" int not null, "boost_days" int not null, "plan_id" varchar(255) not null, "order_id" int not null, "points" int not null);');
    this.addSql('alter table "employer_paid_job_subscription" add constraint "employer_paid_job_subscription_order_id_unique" unique ("order_id");');

    this.addSql('create table "job_boost" ("id" varchar(255) not null, "job_id" int null, "subscription_id" int not null, "employer_user_id" int not null, "boost_start_date" timestamptz null, "boost_end_date" timestamptz null, "valid_for" int not null, "points" int not null, "is_deleted" boolean not null default false, constraint "job_boost_pkey" primary key ("id"));');
    this.addSql('alter table "job_boost" add constraint "job_boost_job_id_subscription_id_unique" unique ("job_id", "subscription_id");');

    this.addSql('create table "employer_job_seeker_subscription" ("id" serial primary key, "employer_user_id" int not null, "valid_for_days" int not null, "created_at" timestamptz not null, "updated_at" timestamptz not null, "expiry_date" timestamptz not null, "allowed_profile_view_count" int not null, "subscription_order_id" int not null, "plan_id" varchar(255) not null);');
    this.addSql('alter table "employer_job_seeker_subscription" add constraint "employer_job_seeker_subscription_subscription_order_id_unique" unique ("subscription_order_id");');

    this.addSql('create table "employer_profile_views" ("id" varchar(255) not null, "created_at" timestamptz not null, "updated_at" timestamptz not null, "employer_user_id" int not null, "job_seeker_user_id" int not null, "job_seeker_data" jsonb not null, "employer_subscription_id" int not null, constraint "employer_profile_views_pkey" primary key ("id"));');
    this.addSql('alter table "employer_profile_views" add constraint "employer_profile_views_employer_user_id_job_seeker_54722_unique" unique ("employer_user_id", "job_seeker_user_id");');

    this.addSql('create table "employer_job_seeker_invite" ("id" varchar(255) not null, "created_at" timestamptz not null, "updated_at" timestamptz not null, "employer_user_id" int not null, "is_applied" boolean not null default false, "email_status" text check ("email_status" in (\'Pending\', \'Success\')) not null, "job_seeker_user_id" int not null, "job_id" int not null, constraint "employer_job_seeker_invite_pkey" primary key ("id"));');

    this.addSql('alter table "paid_job_subscription_order" add constraint "paid_job_subscription_order_employer_user_id_foreign" foreign key ("employer_user_id") references "employer" ("user_id") on update cascade;');
    this.addSql('alter table "paid_job_subscription_order" add constraint "paid_job_subscription_order_plan_id_foreign" foreign key ("plan_id") references "paid_job_subscription_plan" ("id") on update cascade;');

    this.addSql('alter table "job_seeker_subscription_order" add constraint "job_seeker_subscription_order_employer_user_id_foreign" foreign key ("employer_user_id") references "employer" ("user_id") on update cascade;');
    this.addSql('alter table "job_seeker_subscription_order" add constraint "job_seeker_subscription_order_plan_id_foreign" foreign key ("plan_id") references "job_seeker_subscription_plan" ("id") on update cascade;');

    this.addSql('alter table "employer_paid_job_subscription" add constraint "employer_paid_job_subscription_employer_user_id_foreign" foreign key ("employer_user_id") references "employer" ("user_id") on update cascade;');
    this.addSql('alter table "employer_paid_job_subscription" add constraint "employer_paid_job_subscription_plan_id_foreign" foreign key ("plan_id") references "paid_job_subscription_plan" ("id") on update cascade;');
    this.addSql('alter table "employer_paid_job_subscription" add constraint "employer_paid_job_subscription_order_id_foreign" foreign key ("order_id") references "paid_job_subscription_order" ("id") on update cascade;');

    this.addSql('alter table "job_boost" add constraint "job_boost_job_id_foreign" foreign key ("job_id") references "job" ("id") on update cascade on delete set null;');
    this.addSql('alter table "job_boost" add constraint "job_boost_subscription_id_foreign" foreign key ("subscription_id") references "employer_paid_job_subscription" ("id") on update cascade;');
    this.addSql('alter table "job_boost" add constraint "job_boost_employer_user_id_foreign" foreign key ("employer_user_id") references "employer" ("user_id") on update cascade;');

    this.addSql('alter table "employer_job_seeker_subscription" add constraint "employer_job_seeker_subscription_employer_user_id_foreign" foreign key ("employer_user_id") references "employer" ("user_id") on update cascade;');
    this.addSql('alter table "employer_job_seeker_subscription" add constraint "employer_job_seeker_subscription_subscription_order_id_foreign" foreign key ("subscription_order_id") references "job_seeker_subscription_order" ("id") on update cascade;');
    this.addSql('alter table "employer_job_seeker_subscription" add constraint "employer_job_seeker_subscription_plan_id_foreign" foreign key ("plan_id") references "job_seeker_subscription_plan" ("id") on update cascade;');

    this.addSql('alter table "employer_profile_views" add constraint "employer_profile_views_employer_user_id_foreign" foreign key ("employer_user_id") references "employer" ("user_id") on update cascade;');
    this.addSql('alter table "employer_profile_views" add constraint "employer_profile_views_job_seeker_user_id_foreign" foreign key ("job_seeker_user_id") references "job_seeker" ("user_id") on update cascade;');
    this.addSql('alter table "employer_profile_views" add constraint "employer_profile_views_employer_subscription_id_foreign" foreign key ("employer_subscription_id") references "employer_job_seeker_subscription" ("id") on update cascade;');

    this.addSql('alter table "employer_job_seeker_invite" add constraint "employer_job_seeker_invite_employer_user_id_foreign" foreign key ("employer_user_id") references "employer" ("user_id") on update cascade;');
    this.addSql('alter table "employer_job_seeker_invite" add constraint "employer_job_seeker_invite_job_seeker_user_id_foreign" foreign key ("job_seeker_user_id") references "job_seeker" ("user_id") on update cascade;');
    this.addSql('alter table "employer_job_seeker_invite" add constraint "employer_job_seeker_invite_job_id_foreign" foreign key ("job_id") references "job" ("id") on update cascade;');
  }

}
