import { ApiProperty } from '@nestjs/swagger';

export class AssignOrderDto {
  @ApiProperty({ example: '507f1f77bcf86cd799439011', description: 'ID người thực hiện' })
  nguoiThucHien: string;
}
