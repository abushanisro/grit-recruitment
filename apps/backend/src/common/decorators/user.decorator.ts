import {
  applyDecorators,
  createParamDecorator,
  ExecutionContext,
  SetMetadata,
  UseGuards,
} from '@nestjs/common';
import { ApiBearerAuth, ApiUnauthorizedResponse } from '@nestjs/swagger';
import { JwtAuthGuard } from '../../auth/jwt-auth.guard';
import {
  OptionalJwtAuthGuard,
  RolesGuard,
} from '../../user/entities/roles.guard';
import { User } from 'src/user/entities/user.entity';
import { UserRole } from 'contract/enum';

export const user = createParamDecorator(
  (_data, ctx: ExecutionContext): User => {
    const request = ctx.switchToHttp().getRequest();
    const u: User = request.user;
    return u;
  },
);

export function Auth() {
  return applyDecorators(
    UseGuards(JwtAuthGuard),
    ApiBearerAuth('JWT'),
    ApiUnauthorizedResponse({ description: 'Unauthorized' }),
  );
}

export const getUserFromToken = user;
export const ROLES_KEY = 'roles';

export const OptionalUserFromToken = user;

export function EmployerOnlyAuth() {
  return applyDecorators(
    UseGuards(JwtAuthGuard, RolesGuard),
    ApiBearerAuth('JWT'),
    SetMetadata(ROLES_KEY, UserRole.employer),
    ApiUnauthorizedResponse({ description: 'Unauthorized' }),
  );
}

export function JobSeekerOnlyAuth() {
  return applyDecorators(
    UseGuards(JwtAuthGuard, RolesGuard),
    ApiBearerAuth('JWT'),
    SetMetadata(ROLES_KEY, UserRole.jobSeeker),
    ApiUnauthorizedResponse({ description: 'Unauthorized' }),
  );
}

export function AdminOnlyAuth() {
  return applyDecorators(
    UseGuards(JwtAuthGuard, RolesGuard),
    ApiBearerAuth('JWT'),
    SetMetadata(ROLES_KEY, UserRole.admin),
    ApiUnauthorizedResponse({ description: 'Unauthorized' }),
  );
}

export function OptionalAuth() {
  return applyDecorators(
    UseGuards(OptionalJwtAuthGuard),
    ApiBearerAuth('JWT'),
    ApiUnauthorizedResponse({ description: 'Unauthorized' }),
  );
}
