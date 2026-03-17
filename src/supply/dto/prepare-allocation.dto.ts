import { IsArray, ValidateNested, IsMongoId, IsDateString } from 'class-validator';
import { Type } from 'class-transformer';
import { ApiProperty } from '@nestjs/swagger';

export class SupplyExpiryDto {
  @ApiProperty({ description: 'ID vật tư', example: '507f1f77bcf86cd799439011' })
  @IsMongoId()
  vatTu: string;

  @ApiProperty({ description: 'Hạn sử dụng (ISO date)', example: '2025-12-31' })
  @IsDateString()
  hanSuDung: string;
}

export class PrepareAllocationDto {
  @ApiProperty({ description: 'Danh sách hạn sử dụng cho từng vật tư', type: [SupplyExpiryDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => SupplyExpiryDto)
  danhSachHanSuDung: SupplyExpiryDto[];
}
