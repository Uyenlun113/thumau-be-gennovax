import { ApiProperty } from '@nestjs/swagger';
import { IsNotEmpty, IsString, IsOptional } from 'class-validator';

export class LoginDto {
  @ApiProperty({ description: 'Tên đăng nhập hoặc mã nhân viên', example: 'admin' })
  @IsNotEmpty({ message: 'Tên đăng nhập không được để trống' })
  @IsString()
  username: string;

  @ApiProperty({ description: 'Mật khẩu', example: '123456' })
  @IsNotEmpty({ message: 'Mật khẩu không được để trống' })
  @IsString()
  password: string;

  // Thông tin thiết bị (optional)
  @ApiProperty({ description: 'Địa chỉ IP', required: false })
  @IsOptional()
  @IsString()
  ipAddress?: string;

  @ApiProperty({ description: 'User Agent', required: false })
  @IsOptional()
  @IsString()
  userAgent?: string;

  @ApiProperty({ description: 'Loại thiết bị (mobile, desktop, tablet)', required: false })
  @IsOptional()
  @IsString()
  deviceType?: string;

  @ApiProperty({ description: 'Trình duyệt', required: false })
  @IsOptional()
  @IsString()
  browser?: string;

  @ApiProperty({ description: 'Hệ điều hành', required: false })
  @IsOptional()
  @IsString()
  os?: string;

  @ApiProperty({ description: 'Tên thiết bị', required: false })
  @IsOptional()
  @IsString()
  deviceName?: string;

  @ApiProperty({ description: 'Vị trí', required: false })
  @IsOptional()
  @IsString()
  location?: string;
}

