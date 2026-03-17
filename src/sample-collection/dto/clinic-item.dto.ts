import { IsMongoId, IsNotEmpty, IsNumber, Min, IsArray, IsString } from 'class-validator';

export class ClinicItemDto {
  @IsMongoId()
  @IsNotEmpty()
  phongKham: string;

  @IsNumber()
  @Min(0)
  soTienCuocNhanMau: number;

  @IsNumber()
  @Min(0)
  soTienShip: number;

  @IsNumber()
  @Min(0)
  soTienGuiXe: number;

  @IsArray()
  @IsString({ each: true })
  anhHoanThanh: string[];
}
