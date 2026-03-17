import { IsString, IsOptional, IsDateString, IsMongoId } from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class ConfirmDeliveryDto {
  @ApiProperty({ description: 'Ngày giao hàng (ISO date)', example: '2026-02-06' })
  @IsDateString()
  ngayGiao: string;

  @ApiPropertyOptional({ description: 'Ghi chú giao hàng', example: 'Đã giao cho BS An' })
  @IsOptional()
  @IsString()
  ghiChu?: string;

  @ApiPropertyOptional({ description: 'URL ảnh giao nhận', example: '/uploads/deliveries/delivery-123456.jpg' })
  @IsOptional()
  @IsString()
  anhGiaoNhan?: string;

  @ApiProperty({ description: 'ID người giao hàng', example: '507f1f77bcf86cd799439011' })
  @IsMongoId()
  nguoiGiaoHang: string;
}
