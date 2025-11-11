import { Module } from '@nestjs/common';
import { AuthService } from './auth.service';
import { AuthController } from './auth.controller';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { User } from 'src/user/entities/user.entity';
import { EmailInvite } from './entities/emailInvite.entity';
import { GoogleUser } from './entities/google.user.entity';
import { JwtStrategy } from './jwt.strategy';
import { GoogleStrategy } from './google.strategy';
import { JwtModule } from '@nestjs/jwt';
import { jwtConstants } from './constants';
import { EmailService } from './email.service';
import { UserService } from 'src/user/user.service';
import {
  City,
  Industry,
  Language,
  Skill,
  Subfunction,
} from 'src/user/entities/info.entity';
import { Company } from 'src/job/entities/company.entity';

@Module({
  imports: [
    MikroOrmModule.forFeature({
      entities: [
        User,
        EmailInvite,
        GoogleUser,
        City,
        Language,
        Subfunction,
        Industry,
        Company,
        Skill,
      ],
    }),
    JwtModule.registerAsync({
      useFactory: () => ({
        secret: jwtConstants.secret,
        signOptions: { expiresIn: '14d' },
      }),
    }),
  ],
  controllers: [AuthController],
  providers: [
    AuthService,
    JwtStrategy,
    GoogleStrategy,
    EmailService,
    UserService,
  ],
  exports: [AuthService],
})
export class AuthModule {}
