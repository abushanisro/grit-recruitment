import { InjectRepository } from '@mikro-orm/nestjs';
import { EntityManager, EntityRepository } from '@mikro-orm/postgresql';
import {
  BadRequestException,
  HttpStatus,
  Injectable,
  InternalServerErrorException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { User } from 'src/user/entities/user.entity';
import { EmailService } from './email.service';
import { EmailInvite } from './entities/emailInvite.entity';
import { GoogleUser } from './entities/google.user.entity';
import { Request } from 'express';
import { AuthRO, GoogleUserRO } from './auth.ro';
import { v4 as uuidv4 } from 'uuid';
import { ConfigService } from '@nestjs/config';
import * as dayjs from 'dayjs';
import { wrap } from '@mikro-orm/core';
import { nestControllerContract, NestRequestShapes } from '@ts-rest/nest';
import { authContract } from 'contract/auth/contract';
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino';
import { UserRole } from 'contract/enum';
export const authContractController = nestControllerContract(authContract);
export type AuthRequestShapes = NestRequestShapes<
  typeof authContractController
>;

@Injectable()
export class AuthService {
  /* eslint-disable max-params */
  constructor(
    private jwtService: JwtService,

    @InjectRepository(User)
    private readonly userRepository: EntityRepository<User>,

    @InjectRepository(GoogleUser)
    private readonly googleUserRepository: EntityRepository<GoogleUser>,

    @InjectRepository(EmailInvite)
    private readonly emailInviteRepository: EntityRepository<EmailInvite>,

    private readonly emailService: EmailService,

    private em: EntityManager,

    private configService: ConfigService,
    @InjectPinoLogger(AuthService.name)
    private logger: PinoLogger,
  ) {}
  frontEndUrl = this.configService.get<string>('frontEndUrl');
  adminFrontEndUrl = this.configService.get<string>('adminFrontEndUrl');

  getJwtToken(id: number, email: string) {
    const payload = {
      userId: id,
      email: email,
    };
    return this.jwtService.sign(payload);
  }
  async handleGoogleSignin(req: Request, redirectUrl: string | undefined) {
    try {
      const googleUserData = req.user as {
        email: string | null;
        firstName: string;
        lastName: string | null;
        picture: string | null;
      };
      if (!googleUserData.email) {
        throw new InternalServerErrorException(
          'Something went wrong, Please try again',
        );
      }

      const user = await this.userRepository.findOne({
        email: googleUserData.email,
      });

      if (user) {
        const jwt = this.getJwtToken(user.id, user.email);
        if (user.role === UserRole.admin) {
          return {
            statusCode: HttpStatus.FOUND,
            url:
              this.adminFrontEndUrl +
              `/login/?token=${jwt}` +
              (redirectUrl
                ? `&redirectUrl=${encodeURIComponent(redirectUrl)}`
                : ''),
          };
        }
        return {
          statusCode: HttpStatus.FOUND,
          url:
            this.frontEndUrl +
            `/login/?token=${jwt}&role=${user.role}` +
            (redirectUrl
              ? `&redirectUrl=${encodeURIComponent(redirectUrl)}`
              : ''),
        };
      }

      const googleUserExists = await this.googleUserRepository.findOne({
        email: googleUserData.email,
      });

      let refId = googleUserExists?.referenceId;

      if (!googleUserExists) {
        const newGoogleUser = new GoogleUser({
          firstName: googleUserData.firstName,
          lastName: googleUserData.lastName ?? '',
          email: googleUserData.email,
          referenceId: uuidv4(),
          picture: googleUserData.picture ?? '',
        });
        refId = newGoogleUser.referenceId;
        this.em.persist(newGoogleUser);
        await this.em.flush();
      }

      return {
        statusCode: HttpStatus.FOUND,
        url:
          this.frontEndUrl +
          `/signup/?refId=${refId}` +
          (redirectUrl
            ? `&redirectUrl=${encodeURIComponent(redirectUrl)}`
            : ''),
      };
    } catch (error) {
      this.logger.error(error);
      return {
        statusCode: HttpStatus.FOUND,
        url: this.frontEndUrl + `/?googleLoginError=true`,
      };
    }
  }

  async sendInvite(dto: AuthRequestShapes['sendInvite']['body']) {
    const user = await this.userRepository.findOne({
      email: dto.email,
    });

    if (!user) {
      throw new BadRequestException(
        'This email id is not registered. Please Sign up',
      );
    }

    const id = uuidv4();

    const invite = new EmailInvite({ user, refId: id });

    await this.em.persistAndFlush(invite);

    await this.emailService.sendUserEmail(
      {
        MagicLinkURL: this.frontEndUrl + '/loginlink/' + invite.refId,
        JobSeekerName: user.firstName,
      },
      user.email,
      'jobverse_stage_email_invite_jobseeker',
    );

    return {
      status: 200 as const,
      body: { isSuccess: true, message: 'Success' },
    };
  }
  async handleLogin(refId: string): Promise<AuthRO> {
    const invite = await this.emailInviteRepository.findOne(
      {
        refId: refId,
        isUsed: false,
      },
      { populate: ['user'] },
    );

    if (!invite || dayjs().diff(dayjs(invite.createdAt), 'hour') >= 1) {
      throw new BadRequestException(
        'Login link expired. Please create a new login link!',
      );
    }

    const token = this.getJwtToken(invite.user.id, invite.user.email);

    wrap(invite).assign({ isUsed: true });

    await this.em.flush();

    return new AuthRO(token);
  }

  async getGoogleUserData(refId: string) {
    const googleUser = await this.googleUserRepository.findOneOrFail({
      referenceId: refId,
    });
    return new GoogleUserRO(googleUser);
  }
}
