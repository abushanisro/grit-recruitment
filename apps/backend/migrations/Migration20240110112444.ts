import { Migration } from '@mikro-orm/migrations';

export class Migration20240110112444 extends Migration {
  async up(): Promise<void> {
    this.addSql(
      'create table if not exists language(id         serial primary key, created_at timestamp(0) with time zone not null, updated_at timestamp(0) with time zone not null,name       varchar(255)                not null);',
    );
  }
}
