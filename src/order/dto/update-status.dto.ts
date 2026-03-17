import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

class CoordinatesDto {
  @ApiProperty({ example: 10.762622, description: 'Latitude' })
  lat: number;

  @ApiProperty({ example: 106.660172, description: 'Longitude' })
  lng: number;
}

export class UpdateOrderStatusDto {
  @ApiProperty({ 
    example: 'DANG_THUC_HIEN', 
    description: 'Trạng thái mới',
    enum: ['CHO_DIEU_PHOI', 'CHO_NHAN_LENH', 'DANG_THUC_HIEN', 'HOAN_THANH', 'DA_HUY']
  })
  trangThai: string;

  @ApiPropertyOptional({ type: CoordinatesDto, description: 'Tọa độ hiện tại' })
  toaDo?: CoordinatesDto;
}
