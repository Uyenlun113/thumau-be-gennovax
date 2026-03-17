import { IsString, IsArray, IsEnum, IsOptional, ValidateNested, IsMongoId, IsNumber, IsDateString, Min } from 'class-validator';
import { Type } from 'class-transformer';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { DeliveryMethod } from '../schemas/supply-allocation.schema';

export class SupplyItemDto {
  @ApiProperty({ description: 'ID vật tư', example: '507f1f77bcf86cd799439011' })
  @IsMongoId()
  vatTu: string;

  @ApiProperty({ description: 'Tên vật tư', example: 'Bộ dụng cụ CELLPREP' })
  @IsString()
  tenVatTu: string;

  @ApiProperty({ description: 'Số lượng cấp phát', example: 10, minimum: 1 })
  @IsNumber()
  @Min(1)
  soLuong: number;

  @ApiPropertyOptional({ description: 'Hạn sử dụng (ISO date)', example: '2025-12-31' })
  @IsOptional()
  @IsDateString()
  hanSuDung?: string;
}

export class CreateAllocationDto {
  @ApiProperty({ description: 'ID người tạo phiếu', example: '507f1f77bcf86cd799439011' })
  @IsMongoId()
  nguoiTaoPhieu: string;

  @ApiProperty({ description: 'ID phòng khám', example: '507f1f77bcf86cd799439011' })
  @IsMongoId()
  phongKham: string;

  @ApiProperty({ description: 'Hình thức vận chuyển', enum: DeliveryMethod, example: DeliveryMethod.CAP_TAN_NOI })
  @IsEnum(DeliveryMethod)
  hinhThucVanChuyen: DeliveryMethod;

  @ApiProperty({ description: 'Danh sách vật tư cấp phát', type: [SupplyItemDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => SupplyItemDto)
  danhSachVatTu: SupplyItemDto[];
}
