import { Controller } from '@nestjs/common';
import { AnalyticEventService } from './analytic-event.service';
import {
  NestControllerInterface,
  NestRequestShapes,
  TsRest,
  TsRestRequest,
  nestControllerContract,
} from '@ts-rest/nest';
import { contract } from 'contract';
import { Auth, getUserFromToken } from 'src/common/decorators/user.decorator';
import { User } from 'src/user/entities/user.entity';

export const analyticEventController = nestControllerContract(
  contract.analyticEvent,
);
export type AnalyticEventShapes = NestRequestShapes<
  typeof analyticEventController
>;

@Controller('')
export class AnalyticEventController
  implements NestControllerInterface<typeof analyticEventController> {
  constructor(private readonly analyticEventService: AnalyticEventService) {}

  @Auth()
  @TsRest(analyticEventController.createAnalyticEvent)
  async createAnalyticEvent(
    @getUserFromToken() user: User,
    @TsRestRequest() { body }: AnalyticEventShapes['createAnalyticEvent'],
  ) {
    await this.analyticEventService.createAnalyticEvent(user, body);
    return {
      status: 200 as const,
      body: {
        isSuccess: true,
        message: 'Analytic Event Created Successfully!!!!!',
      },
    };
  }
}
