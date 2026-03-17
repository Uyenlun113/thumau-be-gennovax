import { Controller, Get, Post, Delete, Param, Body, UseGuards } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiBearerAuth } from '@nestjs/swagger';
import { NotificationService } from './notification.service';
import { CreateNotificationDto } from './dto/create-notification.dto';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { PermissionsGuard } from '../auth/guards/permissions.guard';
import { Permissions } from '../auth/decorators/permissions.decorator';
import { Permission } from '../role/schemas/role.schema';

@ApiTags('notifications')
@ApiBearerAuth('JWT-auth')
@Controller('notifications')
@UseGuards(JwtAuthGuard, PermissionsGuard)
export class NotificationController {
  constructor(private readonly notificationService: NotificationService) {}

  @Post()
  @Permissions(Permission.NOTIFICATION_CREATE)
  @ApiOperation({ summary: 'Tạo thông báo mới' })
  @ApiResponse({ status: 201, description: 'Thông báo đã được tạo' })
  create(@Body() createNotificationDto: CreateNotificationDto) {
    return this.notificationService.create(createNotificationDto);
  }

  @Get('user/:userId')
  @ApiOperation({ summary: 'Lấy danh sách thông báo của user' })
  @ApiResponse({ status: 200, description: 'Danh sách thông báo' })
  findByUserId(@Param('userId') userId: string) {
    return this.notificationService.findByUserId(userId);
  }

  @Get('user/:userId/unread-count')
  @ApiOperation({ summary: 'Đếm số thông báo chưa đọc' })
  @ApiResponse({ status: 200, description: 'Số lượng thông báo chưa đọc' })
  async getUnreadCount(@Param('userId') userId: string) {
    const count = await this.notificationService.getUnreadCount(userId);
    return { count };
  }

  @Post(':id/read')
  @ApiOperation({ summary: 'Đánh dấu đã đọc thông báo' })
  @ApiResponse({ status: 200, description: 'Đã đánh dấu đọc' })
  markAsRead(@Param('id') id: string) {
    return this.notificationService.markAsRead(id);
  }

  @Post('user/:userId/read-all')
  @ApiOperation({ summary: 'Đánh dấu đã đọc tất cả thông báo' })
  @ApiResponse({ status: 200, description: 'Đã đánh dấu đọc tất cả' })
  async markAllAsRead(@Param('userId') userId: string) {
    await this.notificationService.markAllAsRead(userId);
    return { message: 'Đã đánh dấu đọc tất cả thông báo' };
  }

  @Delete(':id')
  @ApiOperation({ summary: 'Xóa thông báo' })
  @ApiResponse({ status: 200, description: 'Đã xóa thông báo' })
  async delete(@Param('id') id: string) {
    await this.notificationService.delete(id);
    return { message: 'Đã xóa thông báo' };
  }

  @Delete('user/:userId/all')
  @ApiOperation({ summary: 'Xóa tất cả thông báo của user' })
  @ApiResponse({ status: 200, description: 'Đã xóa tất cả thông báo' })
  async deleteAll(@Param('userId') userId: string) {
    await this.notificationService.deleteAll(userId);
    return { message: 'Đã xóa tất cả thông báo' };
  }
}
