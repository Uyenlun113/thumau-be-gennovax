import {
  Body,
  Controller,
  Delete,
  Get,
  Post,
  Put,
  Request,
  UseGuards,
} from '@nestjs/common';
import { ApiBearerAuth, ApiOperation, ApiTags } from '@nestjs/swagger';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { RegisterDeviceDto } from './dto/register-device.dto';
import { TestNotificationDto } from './dto/test-notification.dto';
import { UnregisterDeviceDto } from './dto/unregister-device.dto';
import { UpdatePreferencesDto } from './dto/update-preferences.dto';
import { PushNotificationService } from './push-notification.service';

@ApiTags('Push Notifications')
@Controller('push-notifications')
@UseGuards(JwtAuthGuard)
@ApiBearerAuth()
export class PushNotificationController {
  constructor(
    private readonly pushNotificationService: PushNotificationService,
  ) { }

  @Post('register')
  @ApiOperation({ summary: 'Register device for push notifications' })
  async registerDevice(@Request() req, @Body() dto: RegisterDeviceDto) {
    return this.pushNotificationService.registerDevice(req.user._id.toString(), dto);
  }

  @Delete('unregister')
  @ApiOperation({ summary: 'Unregister device from push notifications' })
  async unregisterDevice(@Request() req, @Body() dto: UnregisterDeviceDto) {
    await this.pushNotificationService.unregisterDevice(
      req.user._id.toString(),
      dto.playerId,
    );
    return { message: 'Device unregistered successfully' };
  }

  @Get('devices')
  @ApiOperation({ summary: 'Get user devices' })
  async getUserDevices(@Request() req) {
    return this.pushNotificationService.getUserDevices(req.user._id.toString());
  }

  @Get('preferences')
  @ApiOperation({ summary: 'Get notification preferences' })
  async getPreferences(@Request() req) {
    return this.pushNotificationService.getPreferences(req.user._id.toString());
  }

  @Put('preferences')
  @ApiOperation({ summary: 'Update notification preferences' })
  async updatePreferences(@Request() req, @Body() dto: UpdatePreferencesDto) {
    return this.pushNotificationService.updatePreferences(
      req.user._id.toString(),
      dto,
    );
  }

  @Post('test')
  @ApiOperation({ summary: 'Send test notification (for testing purposes)' })
  async sendTestNotification(@Request() req, @Body() dto: TestNotificationDto) {
    return this.pushNotificationService.sendTestNotification(
      dto.title,
      dto.message,
      {
        userId: dto.userId || req.user._id.toString(),
        playerId: dto.playerId,
        sendToAll: dto.sendToAll,
      },
    );
  }

  @Post('broadcast')
  @ApiOperation({ summary: 'Send notification to all users (Admin only)' })
  async broadcastNotification(@Request() req, @Body() dto: TestNotificationDto) {
    return this.pushNotificationService.sendToAllUsers(
      dto.title,
      dto.message,
      { broadcast: true },
    );
  }
}
