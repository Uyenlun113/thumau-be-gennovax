import { IsString, IsNotEmpty, IsOptional, IsEnum } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class TestNotificationDto {
  @ApiProperty({ description: 'User ID to send test notification', required: false })
  @IsString()
  @IsOptional()
  userId?: string;

  @ApiProperty({ description: 'Player ID to send test notification', required: false })
  @IsString()
  @IsOptional()
  playerId?: string;

  @ApiProperty({ description: 'Notification title' })
  @IsString()
  @IsNotEmpty()
  title: string;

  @ApiProperty({ description: 'Notification message' })
  @IsString()
  @IsNotEmpty()
  message: string;

  @ApiProperty({ description: 'Send to all users', required: false })
  @IsOptional()
  sendToAll?: boolean;
}
