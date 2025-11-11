import { Migration } from '@mikro-orm/migrations';

export class Migration20240220100719 extends Migration {

  async up(): Promise<void> {
    this.addSql('alter table "job" alter column "external_link" type varchar(2000) using ("external_link"::varchar(2000));');
  }

  async down(): Promise<void> {
    this.addSql('alter table "job" alter column "external_link" type varchar(255) using ("external_link"::varchar(255));');
  }

}
