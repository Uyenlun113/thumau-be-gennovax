import { ApiProperty } from '@nestjs/swagger';
import { IsArray, IsString } from 'class-validator';

export class CompleteVerificationDto {
  @ApiProperty({ 
    type: [String], 
    example: ['https://example.com/image1.jpg', 'https://example.com/image2.jpg'], 
    description: 'Ảnh hoàn thành kiểm tra' 
  })
  @IsArray()
  @IsString({ each: true })
  anhHoanThanhKiemTra: string[];

  @ApiProperty({ example: '507f1f77bcf86cd799439011', description: 'ID người thực hiện' })
  @IsString()
  nguoiThucHien: string;
}