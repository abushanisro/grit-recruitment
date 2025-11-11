import { Migration } from '@mikro-orm/migrations';

export class Migration20240503063204 extends Migration {

  async up(): Promise<void> {
    this.addSql('alter table "company" add column "gst_number" text not null, add column "gst_address" text not null;');

    this.addSql('alter table "job" add column "max_experience_in_years" int not null;');
    this.addSql('alter table "job" rename column "experience_in_years" to "min_experience_in_years";');
  }

  async down(): Promise<void> {
    this.addSql('alter table "job" rename column "min_experience_in_years" to "experience_in_years";');
  }

}
