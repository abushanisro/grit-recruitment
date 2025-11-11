import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { Observable } from 'rxjs';
import { User } from 'src/user/entities/user.entity';
import { Job } from './entities/job.entity';
import { EntityManager } from '@mikro-orm/postgresql';
import { SavedJob } from './entities/savedjobs.entity';
import { UserRole } from 'contract/enum';
import * as dayjs from 'dayjs';

@Injectable()
export class JobInterceptor implements NestInterceptor {
  constructor(private em: EntityManager) {}
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const request = context.switchToHttp().getRequest();
    const user: User | null = request.user;
    if (user && user.role === UserRole.employer) {
      this.em.addFilter(
        'createdByEmployer',
        { createdByEmployer: { user: user } },
        Job,
      );
    }

    if (!user || user.role === UserRole.jobSeeker) {
      this.em.addFilter(
        'postedJobs',
        {
          isPosted: true,
          isAdminApproved: true,
          adminApprovedTime: { $gte: dayjs().subtract(30, 'days').toDate() },
        },
        Job,
      );
    }


    this.em.addFilter(
      'deletedJobs',
      {
        isDeleted: false,
      },
      [Job, SavedJob],
    );

    return next.handle().pipe();
  }
}
