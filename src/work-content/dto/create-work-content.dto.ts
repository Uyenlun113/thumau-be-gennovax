import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class CreateWorkContentDto {
  @ApiProperty({ example: 'Nhận mẫu xét nghiệm', description: 'Tên công việc' })
  tenCongViec: string;

  @ApiPropertyOptional({ example: 'Nhận mẫu xét nghiệm tại phòng khám', description: 'Mô tả công việc' })
  moTa?: string;
}
