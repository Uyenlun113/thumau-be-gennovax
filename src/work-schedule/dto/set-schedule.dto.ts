import { ApiProperty } from '@nestjs/swagger';
import { IsString, IsNotEmpty, Matches } from 'class-validator';

export class SetScheduleDto {
  @ApiProperty({ example: '507f1f77bcf86cd799439011', description: 'ID người dùng' })
  @IsString()
  @IsNotEmpty()
  userId: string;

  @ApiProperty({ example: '2024-01-15', description: 'Ngày (YYYY-MM-DD)' })
  @IsString()
  @IsNotEmpty()
  @Matches(/^\d{4}-\d{2}-\d{2}$/, { message: 'Date must be in YYYY-MM-DD format' })
  date: string;

  @ApiProperty({ 
    example: 'FULL_CA', 
    description: 'Ca làm việc (chỉ chọn 1 ca)',
    enum: ['FULL_CA', 'CA_1', 'CA_2', 'CA_3', 'CA_1_CA_2', 'CA_2_CA_3', 'CA_1_CA_3', 'CA_1_CA_2_CA_3', 'MOT_PHAN_2_SANG', 'MOT_PHAN_2_CHIEU', 'OFF_SANG', 'OFF_CHIEU', 'MOT_PHAN', 'OFF', 'NGHI_LE']
  })
  @IsString()
  @IsNotEmpty()
  caLamViec: string;
}
