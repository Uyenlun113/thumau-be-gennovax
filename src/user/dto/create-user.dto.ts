import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

class CoordinatesDto {
  @ApiProperty({ example: 10.762622, description: 'Latitude' })
  lat: number;

  @ApiProperty({ example: 106.660172, description: 'Longitude' })
  lng: number;
}

export class CreateUserDto {
  @ApiPropertyOptional({ example: 'NV001', description: 'Mã nhân viên (tự động sinh nếu không nhập)' })
  maNhanVien?: string;

  @ApiProperty({ example: 'user001', description: 'Tên đăng nhập (unique)' })
  username: string;

  @ApiProperty({ example: 'password123', description: 'Mật khẩu' })
  password: string;

  @ApiProperty({ example: 'Nguyễn Văn A', description: 'Họ tên' })
  hoTen: string;

  @ApiProperty({ example: '507f1f77bcf86cd799439011', description: 'ID vai trò' })
  vaiTro: string;

  @ApiPropertyOptional({ example: 'user@example.com', description: 'Email' })
  email?: string;

  @ApiPropertyOptional({ example: '0901234567', description: 'Số điện thoại' })
  soDienThoai?: string;

  @ApiPropertyOptional({ type: CoordinatesDto, description: 'Vị trí hiện tại' })
  viTriHienTai?: CoordinatesDto;

  @ApiPropertyOptional({ example: true, description: 'Trạng thái hoạt động', default: true })
  dangHoatDong?: boolean;

  @ApiPropertyOptional({
    example: 'FULL_CA',
    description: 'Ca làm việc',
    enum: ['FULL_CA', 'CA_1', 'CA_2', 'CA_3', 'CA_1_CA_2', 'MOT_PHAN_2_SANG', 'MOT_PHAN_2_CHIEU', 'OFF_SANG', 'OFF_CHIEU', 'MOT_PHAN', 'OFF', 'NGHI_LE'],
    default: 'FULL_CA'
  })
  caLamViec?: string;
}
