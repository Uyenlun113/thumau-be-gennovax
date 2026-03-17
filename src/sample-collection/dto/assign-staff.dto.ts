import { ApiProperty } from '@nestjs/swagger';

export class AssignStaffDto {
  @ApiProperty({ example: '507f1f77bcf86cd799439011', description: 'ID nhân viên thực hiện' })
  nhanVienThucHien: string;
}
