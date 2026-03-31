import { IsString, IsNumber, IsOptional, IsEnum, Min } from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { SupplyStatus, SupplyCategory } from '../schemas/supply.schema';

export class CreateSupplyDto {
  @ApiPropertyOptional({ description: 'Mã vật tư (tự động sinh nếu không nhập)', example: 'VT001' })
  @IsOptional()
  @IsString()
  maVatTu?: string;

  @ApiProperty({ description: 'Tên vật tư', example: 'Bộ dụng cụ CELLPREP' })
  @IsString()
  tenVatTu: string;

  @ApiProperty({ description: 'Loại vật tư', enum: SupplyCategory, example: SupplyCategory.CELL })
  @IsEnum(SupplyCategory)
  loaiVatTu: SupplyCategory;

  @ApiPropertyOptional({ description: 'Mô tả chi tiết', example: 'Bộ dụng cụ thu mẫu tế bào cổ tử cung' })
  @IsOptional()
  @IsString()
  moTa?: string;

  @ApiPropertyOptional({ description: 'Đơn vị tính', example: 'Bộ' })
  @IsOptional()
  @IsString()
  donVi?: string;

  @ApiPropertyOptional({ description: 'Số lượng tồn kho', example: 100, default: 0 })
  @IsOptional()
  @IsNumber()
  @Min(0)
  tonKho?: number;

  @ApiPropertyOptional({ description: 'Mức tồn kho tối thiểu', example: 10, default: 0 })
  @IsOptional()
  @IsNumber()
  @Min(0)
  mucToiThieu?: number;

  @ApiPropertyOptional({ description: 'URL hình ảnh vật tư', example: '/uploads/supplies/supply-123456.jpg' })
  @IsOptional()
  @IsString()
  hinhAnh?: string;

  @ApiPropertyOptional({ description: 'Hạn sử dụng', example: '2025-12-31' })
  @IsOptional()
  @IsString()
  hanSuDung?: string;

  @ApiPropertyOptional({ description: 'Trạng thái vật tư', enum: SupplyStatus, example: SupplyStatus.BINH_THUONG })
  @IsOptional()
  @IsEnum(SupplyStatus)
  trangThai?: SupplyStatus;
}
