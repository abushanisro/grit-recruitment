import { EmailService } from './../auth/email.service';
import { EntityManager } from '@mikro-orm/postgresql';
import { Injectable } from '@nestjs/common';
import { CronExpressions, Schedule } from 'src/pgschedule/pgschedule.decorator';
import { SentJobAlert } from './entities/sendJobAlert.entity';
import { QueryResult } from 'pg';
import { Job } from './entities/job.entity';
import { User } from 'src/user/entities/user.entity';
import { ConfigService } from '@nestjs/config';
import { generateSlugForIdPagePath } from 'contract/utils';
import { tipTapExtensions } from 'contract/common';
import { generateHTML } from '@tiptap/html';

interface JobsToSendInterface {
  job_id: number;
  user_id: number;
  email: string;
  user_first_name: string;
  user_last_name: string;
  job_title: string;
  job_employment_types: string[];
  job_description: JSON;
  city_names: string[];
  company_name: string;
}

@Injectable()
export class JobAlertService {
  constructor(
    private readonly em: EntityManager,
    private emailService: EmailService,

    private configService: ConfigService,
  ) {}

  frontEndUrl = this.configService.get<string>('frontEndUrl');

  private tiptapJSONToHTML(description: JSON) {
    return generateHTML(description, tipTapExtensions);
  }

  @Schedule('jobAlerts', CronExpressions.dailyAlertTime)
  async sendJobAlertForJobseeker() {
    const knex = this.em.getKnex();
    const jobsToSend = await knex.raw<QueryResult<JobsToSendInterface>>(
      `with today_already_sent_users as (
        select distinct user_id
        from sent_job_alert
        where created_at >= current_date
          and created_at <= current_date + interval '1 day'
    ),
    
         ju as (
             select distinct on (u.id) u.id               as user_id
                                     , j.id               as job_id
                                     , u.email
                                     , u.first_name       as user_first_name
                                     , u.last_name        as user_last_name
                                     , j.title            as job_title
                                     , j.employment_types as job_employment_types
                                     , j.description      as job_description
                                     , j.company_id
                                     , j.admin_approved_time
             from "user" u
                      inner join job_seeker js on js.user_id = u.id
                      left join job_seeker_preferred_locations jspl on jspl.job_seeker_user_id = js.user_id
                      left join job_seeker_subfunction jssf on jssf.job_seeker_user_id = js.user_id
                      inner join job j on true
                      left join company cpy on j.company_id = cpy.id
                      left join job_cities jc on j.id = jc.job_id
                      left join city c on jc.city_id = c.id
                      left join job_sub_functions jsf on j.id = jsf.job_id
             where ((js.expected_salary_in_lpa
                         > j.min_salary_in_lpa and js.expected_salary_in_lpa
                         < j.max_salary_in_lpa)
                 or (js.work_schedule = any (j.employment_modes))
                 or (jssf.subfunction_id = jsf.subfunction_id)
                 or (jc.city_id = jspl.city_id)
                 )
               and u.role = 'jobSeeker'
               and j.is_external_job = false
               and j.is_deleted = false
               and j.is_posted = true
               and j.is_admin_approved = true
               and j.admin_approved_time >= now() - interval '30 days'
               and js.is_subscribed_to_alerts = true
               and j.created_at
                 > js.created_at
               and not exists(select 1 from today_already_sent_users tasu where tasu.user_id = user_id)
              and not exists(select 1 from sent_job_alert sja where sja.user_id = u.id and sja.job_id = j.id)
             order by user_id
                    , j.created_at desc
         )
    select ju.user_id,
           ju.job_id,
           ju.email,
           ju.user_first_name,
           ju.user_last_name,
           ju.job_title,
           ju.job_employment_types,
           ju.job_description,
           cpy.name          as company_name,
           ju.admin_approved_time,
           array_agg(c.name) as city_names
    
    
    from ju
             left join company cpy on ju.company_id = cpy.id
             left join job_cities jc on ju.job_id = jc.job_id
             left join city c on jc.city_id = c.id
             left join job_sub_functions jsf on ju.job_id = jsf.job_id
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
    
    
    
        `,
    );

    await Promise.all(
      jobsToSend.rows.map(async (j) => {
        const jobAlert = new SentJobAlert({
          job: this.em.map(Job, { id: j.job_id }),
          user: this.em.map(User, { id: j.user_id }),
        });
        await this.emailService.sendJobAlertEmail(
          {
            userName: `${j.user_first_name} ${j.user_last_name}`,
            jobTitle: j.job_title,
            companyName: j.company_name,
            locationName: j.city_names.join(', '),
            jobType: j.job_employment_types.join(', '),
            description: this.tiptapJSONToHTML(j.job_description),
            jobLink: `${this.frontEndUrl}/job/${
              j.job_id
            }/${generateSlugForIdPagePath([
              j.job_title,
              j.company_name,
              j.city_names.join('-'),
              j.job_employment_types.join('-'),
            ])}`,
          },
          j.email,
        );
        this.em.persist(jobAlert);
      }),
    );

    await this.em.flush();
  }
}
