import { Module } from '@nestjs/common';
import { PaidJobsController } from './paidJobs.controller';
import { PaidJobsService } from './paidJobs.service';
import { EmailService } from 'src/auth/email.service';
import { UserModule } from 'src/user/user.module';
import { AuthModule } from 'src/auth/auth.module';
import { SubscriptionService } from 'src/subscription/subscriptions.service';

@Module({
  controllers: [PaidJobsController],
  providers: [PaidJobsService, EmailService, SubscriptionService],
  imports: [UserModule, AuthModule],
  exports: [PaidJobsService],
})
export class PaidJobsModule {}
