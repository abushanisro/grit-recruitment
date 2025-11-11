import { Migration } from '@mikro-orm/migrations';

export class Migration20240627101212 extends Migration {

  async up(): Promise<void> {
    this.addSql('alter table "employer_profile_views" drop constraint "employer_profile_views_employer_user_id_job_seeker_54722_unique";');

    this.addSql('alter table "employer_profile_views" add constraint "employer_profile_views_employer_user_id_job_seeker_47a20_unique" unique ("employer_user_id", "job_seeker_user_id", "employer_subscription_id");');
  }

  async down(): Promise<void> {
    this.addSql('alter table "employer_profile_views" drop constraint "employer_profile_views_employer_user_id_job_seeker_47a20_unique";');

    this.addSql('alter table "employer_profile_views" add constraint "employer_profile_views_employer_user_id_job_seeker_54722_unique" unique ("employer_user_id", "job_seeker_user_id");');
  }

}
