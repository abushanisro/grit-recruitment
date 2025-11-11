import { Module } from '@nestjs/common';
import { JobService } from './job.service';
import { JobController } from './job.controller';
import { UserModule } from 'src/user/user.module';
import { JobAlertService } from './jobAlert.service';
import { AuthModule } from 'src/auth/auth.module';
import { EmailService } from 'src/auth/email.service';
import { PaidJobsModule } from 'src/paidJobs/paidJobs.module';

@Module({
  controllers: [JobController],
  providers: [JobService, JobAlertService, EmailService],
  imports: [UserModule, AuthModule, PaidJobsModule],
})
export class JobModule {}
