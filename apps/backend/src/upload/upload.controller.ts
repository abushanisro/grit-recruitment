import {
  Controller,
  Post,
  UploadedFiles,
  UseInterceptors,
  HttpStatus,
  HttpException,
} from '@nestjs/common';
import { ApiTags, ApiOkResponse } from '@nestjs/swagger';
import { FilesInterceptor } from '@nestjs/platform-express';
import { UploadService } from './upload.service';
import { MediaUploadResponse } from './ro';

@ApiTags('Media Upload')
@Controller('upload')
export class UploadController {
  constructor(private readonly uploadService: UploadService) {}

  @ApiOkResponse({ status: 200, type: MediaUploadResponse })
  @Post('media')
  @UseInterceptors(FilesInterceptor('files'))
  async uploadMedia(
    @UploadedFiles() files: Array<Express.Multer.File> | undefined,
  ) {
    if (files === undefined || files.length < 1) {
      throw new HttpException('Invalid Request', HttpStatus.BAD_REQUEST);
    }

    return await this.uploadService.uploadMedia(files);
  }
}
