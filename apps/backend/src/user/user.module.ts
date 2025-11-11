import { Module } from '@nestjs/common';
import { UserService } from './user.service';
import { UserController } from './user.controller';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { User } from './entities/user.entity';
import {
  City,
  Industry,
  Language,
  Skill,
  Subfunction,
} from './entities/info.entity';
import { Company } from 'src/job/entities/company.entity';
import { AuthModule } from 'src/auth/auth.module';

@Module({
  imports: [
    MikroOrmModule.forFeature({
      entities: [User, City, Language, Subfunction, Industry, Company, Skill],
    }),
    AuthModule,
  ],
  controllers: [UserController],
  providers: [UserService],
  exports: [UserService],
})
export class UserModule {}
