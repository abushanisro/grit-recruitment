import { Injectable, InternalServerErrorException } from '@nestjs/common';
import * as AWS from 'aws-sdk';
import { ConfigService } from '@nestjs/config';
import { MediaUploadResponse, UploadData } from './ro';
import * as sharp from 'sharp';
import * as path from 'path';

@Injectable()
export class UploadService {
  constructor(private configService: ConfigService) {}

  s3 = new AWS.S3({
    accessKeyId: this.configService.get<string>('aws.accessKey') as string,
    secretAccessKey: this.configService.get<string>('aws.secretKey') as string,
    region: this.configService.get<string>('aws.region') as string,
  });
  awsBucketName = this.configService.get<string>('aws.bucket');

  async uploadMedia(files: Express.Multer.File[]) {
    const uploadedFiles = await Promise.all(
      files.map((file) => {
        return this.uploadToS3(file);
      }),
    );

    return new MediaUploadResponse(uploadedFiles);
  }

  private async uploadToS3(file: Express.Multer.File) {
    if (!this.awsBucketName) {
      throw new InternalServerErrorException('Something Went Wrong!');
    }

    let fileBuffer = file.buffer;
    let fileExtension = path.extname(file.originalname);
    let contentType = file.mimetype;
    const isImage = file.mimetype.startsWith('image/');
    if (isImage) {
      let image = sharp(fileBuffer);
      let metadata = await image.metadata();

      if (metadata.size && metadata.size > 100000 && metadata.width) {
        const newWidth = Math.round(metadata.width / 4);
        fileBuffer = await image.resize(newWidth).toBuffer();
        // Update image and metadata
        image = sharp(fileBuffer);
        metadata = await image.metadata();
      }

      fileExtension = '.webp';
      contentType = 'image/webp';
    }
    const fileName = path.basename(
      file.originalname,
      path.extname(file.originalname),
    );
    const newFileName = `${fileName}-${new Date().getTime()}${fileExtension}`;

    const params = {
      Bucket: this.awsBucketName,
      Key: newFileName,
      ContentType: contentType,
      Body: fileBuffer,
      ACL: 'public-read',
      ContentDisposition: 'attachment',
    };

    const s3Response = await this.s3.upload(params).promise();

    if (!s3Response.Location) {
      throw new InternalServerErrorException('File Not Uploaded!');
    }

    return new UploadData({
      url: s3Response.Location,
      key: s3Response.Key,
      type: file.mimetype,
      isUploaded: true,
      message: 'Uploaded',
    });
  }
}
