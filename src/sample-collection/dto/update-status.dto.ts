import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

class CoordinatesDto {
  @ApiProperty({ example: 10.762622, description: 'Latitude' })
  lat: number;

  @ApiProperty({ example: 106.660172, description: 'Longitude' })
  lng: number;
}

export class UpdateSampleCollectionStatusDto {
  @ApiProperty({ 
    example: 'DANG_THUC_HIEN', 
    description: 'Trạng thái mới',
    enum: ['CHO_DIEU_PHOI', 'DANG_THUC_HIEN', 'HOAN_THANH', 'HOAN_THANH_KIEM_TRA', 'DA_HUY']
  })
  trangThai: string;

  @ApiPropertyOptional({ description: 'ID người thực hiện thay đổi' })
  nguoiThucHien?: string;

  @ApiPropertyOptional({ type: CoordinatesDto, description: 'Vị trí hiện tại' })
  viTri?: CoordinatesDto;

  @ApiPropertyOptional({ example: '2024-01-15T10:00:00Z', description: 'Thời gian hoàn thành' })
  thoiGianHoanThanh?: Date;

  @ApiPropertyOptional({ 
    type: [String], 
    example: ['https://example.com/image1.jpg'], 
    description: 'Ảnh hoàn thành' 
  })
  anhHoanThanh?: string[];
}
