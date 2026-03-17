import { IsString, IsNotEmpty, IsEnum, IsOptional } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class RegisterDeviceDto {
  @ApiProperty({ description: 'OneSignal Player ID' })
  @IsString()
  @IsNotEmpty()
  playerId: string;

  @ApiProperty({ enum: ['ios', 'android', 'web'] })
  @IsEnum(['ios', 'android', 'web'])
  platform: 'ios' | 'android' | 'web';

  @ApiProperty({ required: false })
  @IsString()
  @IsOptional()
  deviceModel?: string;

  @ApiProperty({ required: false })
  @IsString()
  @IsOptional()
  appVersion?: string;

  @ApiProperty({ required: false })
  @IsString()
  @IsOptional()
  osVersion?: string;
}
