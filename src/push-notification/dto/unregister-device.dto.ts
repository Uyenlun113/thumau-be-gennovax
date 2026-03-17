import { IsString, IsNotEmpty } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class UnregisterDeviceDto {
  @ApiProperty({ description: 'OneSignal Player ID' })
  @IsString()
  @IsNotEmpty()
  playerId: string;
}
