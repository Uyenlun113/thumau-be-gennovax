import { IsNumber, IsString, IsOptional, IsMongoId } from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class AdjustStockDto {
  @ApiProperty({ description: 'Số lượng điều chỉnh (dương: thêm, âm: bớt)', example: 50 })
  @IsNumber()
  soLuong: number;

  @ApiPropertyOptional({ description: 'Lý do điều chỉnh', example: 'Nhập kho từ NCC' })
  @IsOptional()
  @IsString()
  lyDo?: string;

  @ApiProperty({ description: 'ID người thực hiện', example: '507f1f77bcf86cd799439011' })
  @IsMongoId()
  nguoiThucHien: string;
}
