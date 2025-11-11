import { Enum, ManyToOne, PrimaryKey, Property } from '@mikro-orm/core';
import { EmailStatusEnum } from 'contract/enum';
import { Employer } from 'src/user/entities/employer.entity';
import { v4 } from 'uuid';

export abstract class CommonJobInvite {
  @PrimaryKey()
  id!: string;

  @Property()
  createdAt = new Date();

  @Property({ onUpdate: () => new Date() })
  updatedAt = new Date();

  @ManyToOne(() => Employer)
  employer: Employer;

  @Property({ default: false })
  isApplied: boolean;

  @Enum({ items: () => EmailStatusEnum })
  emailStatus: EmailStatusEnum;

  constructor({ employer }: { employer: Employer }) {
    this.employer = employer;
    this.isApplied = false;
    this.id = v4();
    this.emailStatus = EmailStatusEnum.Pending;
  }
}
