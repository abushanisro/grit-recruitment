import { Migration } from '@mikro-orm/migrations';

export class Migration20240214065558 extends Migration {

  async up(): Promise<void> {
    this.addSql('alter table "job" alter column "description" type jsonb using ("description"::jsonb);');
  }

  async down(): Promise<void> {
    this.addSql('alter table "job" alter column "description" type varchar(2000) using ("description"::varchar(2000));');
  }

}
