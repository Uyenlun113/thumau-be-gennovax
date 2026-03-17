import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

class CoordinatesDto {
  @ApiProperty({ example: 10.762622, description: 'Latitude' })
  lat: number;

  @ApiProperty({ example: 106.660172, description: 'Longitude' })
  lng: number;
}

export class CreateOrderDto {
  @ApiProperty({ example: 'PC001', description: 'Mã phiếu cấp (unique)' })
  maPhieuCap: string;

  @ApiProperty({ example: 'Phòng khám ABC', description: 'Tên phòng khám' })
  phongKham: string;

  @ApiProperty({ example: '123 Nguyễn Văn Linh, Q7', description: 'Địa điểm' })
  diaDiem: string;

  @ApiProperty({ example: '2024-01-01', description: 'Từ ngày' })
  tuNgay: Date;

  @ApiProperty({ example: '2024-01-31', description: 'Đến ngày' })
  denNgay: Date;

  @ApiPropertyOptional({ 
    example: 'CHO_DIEU_PHOI', 
    description: 'Trạng thái',
    enum: ['CHO_DIEU_PHOI', 'CHO_NHAN_LENH', 'DANG_THUC_HIEN', 'HOAN_THANH', 'DA_HUY'],
    default: 'CHO_DIEU_PHOI'
  })
  trangThai?: string;

  @ApiPropertyOptional({ example: 'Găng tay y tế', description: 'Tên vật tư' })
  tenVatTu?: string;

  @ApiPropertyOptional({ example: 100, description: 'Số lượng cấp', default: 0 })
  soLuongCap?: number;

  @ApiPropertyOptional({ example: 50, description: 'Số lượng đã dùng', default: 0 })
  soLuongDaDung?: number;

  @ApiPropertyOptional({ example: 50, description: 'Số lượng tồn', default: 0 })
  soLuongTon?: number;

  @ApiPropertyOptional({ example: 50, description: 'Tỷ lệ sử dụng (%)', default: 0 })
  tyLeSuDung?: number;

  @ApiPropertyOptional({ example: 'Ghi chú thêm', description: 'Ghi chú' })
  ghiChu?: string;

  @ApiPropertyOptional({ example: 'Nguyễn Văn A', description: 'Người tạo' })
  nguoiTao?: string;

  @ApiPropertyOptional({ example: 'Nguyễn Văn B', description: 'Người thực hiện' })
  nguoiThucHien?: string;

  @ApiPropertyOptional({ example: 'clinic@example.com', description: 'Email phòng khám' })
  emailPhongKham?: string;

  @ApiPropertyOptional({ type: CoordinatesDto, description: 'Tọa độ' })
  toaDo?: CoordinatesDto;
}
