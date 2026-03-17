import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class CreateRoleDto {
  @ApiProperty({ example: 'Admin', description: 'Tên vai trò (unique)' })
  tenVaiTro: string;

  @ApiPropertyOptional({ example: 'Quản trị viên hệ thống', description: 'Mô tả vai trò' })
  moTa?: string;
}
