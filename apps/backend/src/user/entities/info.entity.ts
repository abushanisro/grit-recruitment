import { Entity, Property } from '@mikro-orm/core';
import { BaseEntity } from '../../base.entity';

export class CommonInfo extends BaseEntity {
  @Property()
  name: string;
  constructor(name: string) {
    super();
    this.name = name;
  }
}

@Entity()
export class City extends CommonInfo {}

@Entity()
export class Skill extends CommonInfo {}

@Entity()
export class Language extends CommonInfo {}

@Entity()
export class Industry extends CommonInfo {}

@Entity()
export class ToolsAndSoftware extends CommonInfo {}
@Entity()
export class Subfunction extends CommonInfo {}
