import { Migration } from '@mikro-orm/migrations';

export class Migration20240110112444 extends Migration {
  async up(): Promise<void> {
    this.addSql(
      `create function set_admin_approval_time() returns trigger
      language plpgsql
  as
  $$
  BEGIN
      -- Check if is_admin_approved is being set to true
      IF NEW.is_admin_approved AND (OLD.is_admin_approved IS NOT TRUE) THEN
          -- Set admin_approved_time to current timestamp
          NEW.admin_approved_time := CURRENT_TIMESTAMP;
      END IF;
      RETURN NEW;
  END;
  $$;
  
  alter function set_admin_approval_time() owner to postgres;
  
  create function verify_employer_before_approval() returns trigger
    language plpgsql
as
$$
BEGIN
    -- Check if is_admin_approved is being set to true
    IF NEW.is_admin_approved THEN
        -- Check if the employer is verified
        IF (SELECT is_verified FROM employer WHERE user_id = NEW.created_by_employer_user_id) = FALSE THEN
            RAISE EXCEPTION 'Cannot approve job: employer is not verified.';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

alter function verify_employer_before_approval() owner to postgres;

create trigger trigger_job_seeker_set_admin_approval_time
    before update
        of is_admin_approved
    on job
    for each row
execute procedure set_admin_approval_time();


create trigger trigger_verify_employer_before_approval
    before insert or update
        of is_admin_approved
    on job
    for each row
execute procedure verify_employer_before_approval();


  `,
    );
  }
}
