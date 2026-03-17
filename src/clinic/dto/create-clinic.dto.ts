import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

class CoordinatesDto {
  @ApiProperty({ example: 10.762622, description: 'Latitude' })
  lat: number;

  @ApiProperty({ example: 106.660172, description: 'Longitude' })
  lng: number;
}

export class CreateClinicDto {
  @ApiPropertyOptional({ example: 'PK001', description: 'Mã phòng khám (tự động sinh nếu không nhập)' })
  maPhongKham?: string;

  @ApiProperty({ example: 'Phòng khám Đa khoa ABC', description: 'Tên phòng khám' })
  tenPhongKham: string;

  @ApiProperty({ example: '123 Nguyễn Văn Linh, Q7, TP.HCM', description: 'Địa chỉ phòng khám' })
  diaChi: string;

  @ApiPropertyOptional({ type: CoordinatesDto, description: 'Tọa độ phòng khám' })
  toaDo?: CoordinatesDto;

  @ApiPropertyOptional({ example: '0901234567', description: 'Số điện thoại' })
  soDienThoai?: string;

  @ApiPropertyOptional({ example: 'contact@clinic.com', description: 'Email' })
  email?: string;

  @ApiPropertyOptional({ example: 'Nguyễn Văn A', description: 'Người đại diện' })
  nguoiDaiDien?: string;

  @ApiPropertyOptional({ example: 'Nội khoa', description: 'Chuyên khoa' })
  chuyenKhoa?: string;

  @ApiPropertyOptional({ example: true, description: 'Trạng thái hoạt động', default: true })
  dangHoatDong?: boolean;

  @ApiPropertyOptional({ example: 'Ghi chú thêm', description: 'Ghi chú' })
  ghiChu?: string;

  @ApiPropertyOptional({ example: false, description: 'Tự động tạo lệnh thu mẫu có định', default: false })
  tuDongTaoLenh?: boolean;

  @ApiPropertyOptional({
    example: [
      {
        ngayTaoLenhTrongTuan: [1, 2, 3, 4, 5],
        noiDungCongViecMacDinh: '507f1f77bcf86cd799439011',
        ghiChuLenh: 'Ghi chú cho lệnh',
        lenhUuTien: false
      }
    ],
    description: 'Danh sách cấu hình tự động tạo lệnh',
    type: 'array'
  })
  cauHinhTuDongTaoLenh?: Array<{
    ngayTaoLenhTrongTuan: number[];
    noiDungCongViecMacDinh?: string;
    ghiChuLenh?: string;
    lenhUuTien?: boolean;
  }>;

  @ApiPropertyOptional({ example: '507f1f77bcf86cd799439011', description: 'ID nhân viên phụ trách' })
  nhanVienPhuTrach?: string;

  @ApiPropertyOptional({ example: '0901234567', description: 'Số điện thoại liên hệ' })
  sdtLienHe?: string;

  @ApiPropertyOptional({ example: 'https://zalo.me/0901234567', description: 'Link Zalo' })
  linkZalo?: string;

  @ApiPropertyOptional({ example: '08:00', description: 'Giờ mở cửa' })
  gioMoCua?: string;

  @ApiPropertyOptional({ example: '17:00', description: 'Giờ đóng cửa' })
  gioDongCua?: string;

  @ApiPropertyOptional({ example: [1, 2, 3, 4, 5, 6], description: 'Ngày làm việc trong tuần (0=CN, 1=T2, ..., 6=T7)' })
  ngayLamViec?: number[];
}
