import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

class CoordinatesDto {
  @ApiProperty({ example: 10.762622, description: 'Latitude' })
  lat: number;

  @ApiProperty({ example: 106.660172, description: 'Longitude' })
  lng: number;
}

export class CreateSampleCollectionDto {
  @ApiProperty({ example: 'ML001', description: 'Mã lệnh (unique)' })
  maLenh: string;

  @ApiProperty({ example: '507f1f77bcf86cd799439012', description: 'ID nội dung công việc' })
  noiDungCongViec: string;

  @ApiProperty({ example: '507f1f77bcf86cd799439013', description: 'ID người giao lệnh' })
  nguoiGiaoLenh: string;

  @ApiPropertyOptional({ example: '507f1f77bcf86cd799439014', description: 'ID nhân viên thực hiện' })
  nhanVienThucHien?: string;

  @ApiPropertyOptional({ 
    type: [String], 
    example: ['https://example.com/image2.jpg'], 
    description: 'Ảnh hoàn thành kiểm tra' 
  })
  anhHoanThanhKiemTra?: string[];

  @ApiPropertyOptional({ 
    example: 'CHO_DIEU_PHOI', 
    description: 'Trạng thái',
    enum: ['CHO_DIEU_PHOI', 'CHO_NHAN_LENH', 'DANG_THUC_HIEN', 'HOAN_THANH', 'DA_HUY'],
    default: 'CHO_DIEU_PHOI'
  })
  trangThai?: string;

  @ApiPropertyOptional({ example: '2024-01-15T10:00:00Z', description: 'Thời gian hoàn thành' })
  thoiGianHoanThanh?: Date;

  @ApiPropertyOptional({ example: '2024-01-15T11:00:00Z', description: 'Thời gian hoàn thành kiểm tra' })
  thoiGianHoanThanhKiemTra?: Date;

  @ApiPropertyOptional({ example: '507f1f77bcf86cd799439015', description: 'ID phòng khám kiểm tra' })
  phongKhamKiemTra?: string;

  @ApiPropertyOptional({ example: 'Ghi chú thêm', description: 'Ghi chú' })
  ghiChu?: string;

  @ApiPropertyOptional({ type: CoordinatesDto, description: 'Vị trí' })
  viTri?: CoordinatesDto;

  @ApiPropertyOptional({ example: false, description: 'Ưu tiên', default: false })
  uuTien?: boolean;

  @ApiPropertyOptional({ example: '2024-01-15T15:00:00Z', description: 'Thời gian hẹn hoàn thành' })
  thoiGianHenHoanThanh?: Date;

  @ApiPropertyOptional({ example: 'Nhà xe Phương Trang', description: 'Tên nhà xe (cho lệnh nhận mẫu từ nhà xe)' })
  tenNhaXe?: string;

  @ApiPropertyOptional({ example: '123 Đường ABC, Quận 1, TP.HCM', description: 'Địa chỉ nhà xe (cho lệnh nhận mẫu từ nhà xe)' })
  diaChiNhaXe?: string;

  @ApiPropertyOptional({ 
    example: '507f1f77bcf86cd799439011', 
    description: 'ID phòng khám (dùng cho lệnh standard - sẽ tự động tạo phongKhamItems với 1 item)' 
  })
  phongKham?: string;
}
