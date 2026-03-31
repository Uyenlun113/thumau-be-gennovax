import { IsString } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class AutoDeductStockDto {
  @ApiProperty({ description: 'Loại dịch vụ (NIPT, ADN, CELL...)', example: 'NIPT' })
  @IsString()
  serviceType: string;

  @ApiProperty({ description: 'Mã ca', example: '2600061N23' })
  @IsString()
  caseCode: string;

  @ApiProperty({ description: 'Tên dịch vụ', example: 'N23 + GA21' })
  @IsString()
  serviceName: string;

  @ApiProperty({ description: 'ID người thực hiện', example: '507f1f77bcf86cd799439011' })
  @IsString()
  nguoiThucHien: string;
}
