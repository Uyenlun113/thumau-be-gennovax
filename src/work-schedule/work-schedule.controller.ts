import { Body, Controller, Delete, Get, Param, Post, Query, UseGuards } from '@nestjs/common';
import { ApiOperation, ApiParam, ApiQuery, ApiResponse, ApiTags, ApiBearerAuth } from '@nestjs/swagger';
import { SetScheduleDto } from './dto/set-schedule.dto';
import { WorkScheduleService } from './work-schedule.service';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { PermissionsGuard } from '../auth/guards/permissions.guard';
import { Permissions } from '../auth/decorators/permissions.decorator';
import { Permission } from '../role/schemas/role.schema';

@ApiTags('work-schedules')
@ApiBearerAuth('JWT-auth')
@Controller('work-schedules')
@UseGuards(JwtAuthGuard, PermissionsGuard)
export class WorkScheduleController {
  constructor(private readonly workScheduleService: WorkScheduleService) {}

  @Post()
  @Permissions(Permission.SCHEDULE_CREATE)
  @ApiOperation({ summary: 'Đặt lịch làm việc cho nhân viên' })
  @ApiResponse({ status: 201, description: 'Lịch làm việc đã được đặt' })
  setSchedule(@Body() data: SetScheduleDto) {
    return this.workScheduleService.setSchedule(data);
  }

  @Get('month')
  @Permissions(Permission.SCHEDULE_VIEW)
  @ApiOperation({ summary: 'Lấy lịch làm việc theo tháng' })
  @ApiQuery({ name: 'year', description: 'Năm', example: 2024 })
  @ApiQuery({ name: 'month', description: 'Tháng (1-12)', example: 1 })
  @ApiResponse({ status: 200, description: 'Danh sách lịch làm việc' })
  getSchedulesByMonth(
    @Query('year') year: number,
    @Query('month') month: number
  ) {
    return this.workScheduleService.getSchedulesByMonth(Number(year), Number(month));
  }

  @Get('user/:userId')
  @Permissions(Permission.SCHEDULE_VIEW)
  @ApiOperation({ summary: 'Lấy lịch làm việc của nhân viên theo khoảng thời gian' })
  @ApiParam({ name: 'userId', description: 'ID nhân viên' })
  @ApiQuery({ name: 'startDate', description: 'Ngày bắt đầu (YYYY-MM-DD)' })
  @ApiQuery({ name: 'endDate', description: 'Ngày kết thúc (YYYY-MM-DD)' })
  @ApiResponse({ status: 200, description: 'Danh sách lịch làm việc' })
  getSchedulesByUser(
    @Param('userId') userId: string,
    @Query('startDate') startDate: string,
    @Query('endDate') endDate: string
  ) {
    return this.workScheduleService.getSchedulesByUser(userId, startDate, endDate);
  }

  @Get(':userId/:date')
  @Permissions(Permission.SCHEDULE_VIEW)
  @ApiOperation({ summary: 'Lấy lịch làm việc của nhân viên trong ngày' })
  @ApiParam({ name: 'userId', description: 'ID nhân viên' })
  @ApiParam({ name: 'date', description: 'Ngày (YYYY-MM-DD)' })
  @ApiResponse({ status: 200, description: 'Lịch làm việc' })
  getSchedule(
    @Param('userId') userId: string,
    @Param('date') date: string
  ) {
    return this.workScheduleService.getSchedule(userId, date);
  }

  @Delete(':userId/:date')
  @Permissions(Permission.SCHEDULE_DELETE)
  @ApiOperation({ summary: 'Xóa lịch làm việc' })
  @ApiParam({ name: 'userId', description: 'ID nhân viên' })
  @ApiParam({ name: 'date', description: 'Ngày (YYYY-MM-DD)' })
  @ApiResponse({ status: 200, description: 'Đã xóa lịch làm việc' })
  deleteSchedule(
    @Param('userId') userId: string,
    @Param('date') date: string
  ) {
    return this.workScheduleService.deleteSchedule(userId, date);
  }
}
