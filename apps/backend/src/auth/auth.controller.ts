import {
  Controller,
  Get,
  Param,
  Post,
  Query,
  Redirect,
  Req,
  UseGuards,
} from '@nestjs/common';
import { AuthService } from './auth.service';
import { AuthGuard } from '@nestjs/passport';
import { Request } from 'express';
import { AuthRO } from './auth.ro';
import { ApiOkResponse } from '@nestjs/swagger';
import {
  nestControllerContract,
  NestControllerInterface,
  NestRequestShapes,
  TsRest,
  TsRestRequest,
} from '@ts-rest/nest';
import { authContract } from 'contract/auth/contract';

export const authContractController = nestControllerContract(authContract);
export type AuthRequestShapes = NestRequestShapes<
  typeof authContractController
>;

@Controller()
export class AuthController
  implements
    NestControllerInterface<Omit<typeof authContractController, 'users'>>
{
  constructor(private readonly authService: AuthService) {}

  @Get('google')
  @UseGuards(AuthGuard('google'))
  async handleGoogleAuth() {}

  @Redirect()
  @Get('google/redirect')
  @UseGuards(AuthGuard('google'))
  handleGoogleAuthRedirect(
    @Req() req: Request,
    @Query('state') redirectUrl?: string,
  ) {
    return this.authService.handleGoogleSignin(req, redirectUrl);
  }

  @TsRest(authContractController.sendInvite)
  sendInvite(@TsRestRequest() { body }: AuthRequestShapes['sendInvite']) {
    return this.authService.sendInvite(body);
  }

  @ApiOkResponse({ type: AuthRO })
  @Post('/login/:refId')
  handleLogin(@Param('refId') refId: string) {
    return this.authService.handleLogin(refId);
  }
  @TsRest(authContractController.getGoogleUser)
  async getGoogleUser(
    @TsRestRequest() { query }: AuthRequestShapes['getGoogleUser'],
  ) {
    const googleUserData = await this.authService.getGoogleUserData(
      query.refId,
    );
    return {
      status: 200 as const,
      body: googleUserData,
    };
  }
}