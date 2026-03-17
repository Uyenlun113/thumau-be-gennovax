import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

class CoordinatesDto {
  @ApiProperty({ example: 10.762622, description: 'Latitude' })
  lat: number;

  @ApiProperty({ example: 106.660172, description: 'Longitude' })
  lng: number;
}

export class CreateBusStationDto {
  @ApiPropertyOptional({ example: 'NX001', description: 'Mã nhà xe (tự động sinh nếu không nhập)' })
  maNhaXe?: string;

  @ApiProperty({ example: 'Nhà xe Phương Trang', description: 'Tên nhà xe' })
  tenNhaXe: string;

  @ApiProperty({ example: '272 Đề Thám, Q1, TP.HCM', description: 'Địa chỉ nhà xe' })
  diaChi: string;

  @ApiPropertyOptional({ type: CoordinatesDto, description: 'Tọa độ nhà xe' })
  toaDo?: CoordinatesDto;

  @ApiPropertyOptional({ example: '0901234567', description: 'Số điện thoại' })
  soDienThoai?: string;

  @ApiPropertyOptional({ example: 'contact@busstation.com', description: 'Email' })
  email?: string;

  @ApiPropertyOptional({ example: 'Nguyễn Văn A', description: 'Người liên hệ' })
  nguoiLienHe?: string;

  @ApiPropertyOptional({ example: true, description: 'Trạng thái hoạt động', default: true })
  dangHoatDong?: boolean;

  @ApiPropertyOptional({ example: 'Ghi chú thêm', description: 'Ghi chú' })
  ghiChu?: string;
}
