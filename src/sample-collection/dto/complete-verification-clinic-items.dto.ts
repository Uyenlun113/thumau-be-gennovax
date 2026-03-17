import { IsArray, IsMongoId, IsNotEmpty, ValidateNested, ArrayMinSize } from 'class-validator';
import { Type } from 'class-transformer';
import { ClinicItemDto } from './clinic-item.dto';

export class CompleteVerificationClinicItemsDto {
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => ClinicItemDto)
  @ArrayMinSize(1)
  phongKhamItems: ClinicItemDto[];

  @IsMongoId()
  @IsNotEmpty()
  nguoiThucHien: string;
}
