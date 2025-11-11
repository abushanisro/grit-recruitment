import { Migration } from '@mikro-orm/migrations';

export class Migration20240130051525 extends Migration {

  async up(): Promise<void> {
    this.addSql('alter table "user" add constraint "user_phone_unique" unique ("phone");');
  }

  async down(): Promise<void> {
    this.addSql('alter table "user" drop constraint "user_phone_unique";');
  }

}
