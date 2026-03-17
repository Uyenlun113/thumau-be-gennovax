import { ApiProperty } from '@nestjs/swagger';

export class CreateWorkScheduleDto {
  @ApiProperty({ example: '507f1f77bcf86cd799439011', description: 'ID người dùng' })
  userId: string;

  @ApiProperty({ example: '2024-01-15', description: 'Ngày (YYYY-MM-DD)' })
  date: string;

  @ApiProperty({ 
    example: 'FULL_CA', 
    description: 'Ca làm việc',
    enum: ['FULL_CA', 'CA_1', 'CA_2', 'CA_3', 'CA_1_CA_2', 'MOT_PHAN_2_SANG', 'MOT_PHAN_2_CHIEU', 'OFF_SANG', 'OFF_CHIEU', 'MOT_PHAN', 'OFF', 'NGHI_LE']
  })
  caLamViec: string;
}
