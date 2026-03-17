import { IsArray, IsMongoId, IsNotEmpty, IsString } from 'class-validator';

export class CompleteBusStationOrderDto {
  @IsArray()
  @IsString({ each: true })
  anhHoanThanh: string[];

  @IsMongoId()
  @IsNotEmpty()
  nguoiThucHien: string;
}
