import { Module } from '@nestjs/common';
import { JobSeekerSubscriptionService } from './jobSeekerSubscriptions.service';
import { JobSeekerSubscriptionsController } from './jobSeekerSubscriptions.controller';
import { UserModule } from 'src/user/user.module';
import { AuthModule } from 'src/auth/auth.module';
import { EmailService } from 'src/auth/email.service';
import { SubscriptionService } from 'src/subscription/subscriptions.service';

@Module({
  controllers: [JobSeekerSubscriptionsController],
  providers: [JobSeekerSubscriptionService, EmailService, SubscriptionService],
  imports: [UserModule, AuthModule],
})
export class JobSeekerSubscriptionModule {}
