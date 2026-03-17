import { Controller, Post, Body, HttpCode, HttpStatus, Get, Param } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse } from '@nestjs/swagger';
import { AuthService } from './auth.service';
import { LoginDto } from './dto/login.dto';
import { LoginDeviceDto } from './dto/login-device.dto';

@ApiTags('auth')
@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Post('login')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Đăng nhập' })
  @ApiResponse({ status: 200, description: 'Đăng nhập thành công' })
  @ApiResponse({ status: 401, description: 'Tên đăng nhập hoặc mật khẩu không đúng' })
  async login(@Body() loginDto: LoginDto) {
    return this.authService.login(loginDto);
  }

  @Post('save-login-device')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Lưu thông tin thiết bị đăng nhập' })
  @ApiResponse({ status: 200, description: 'Lưu thành công' })
  async saveLoginDevice(@Body() body: { userId: string; device: LoginDeviceDto }) {
    return this.authService.saveLoginDevice(body.userId, body.device);
  }

  @Get('login-devices/:userId')
  @ApiOperation({ summary: 'Lấy danh sách thiết bị đã đăng nhập của 1 user' })
  @ApiResponse({ status: 200, description: 'Lấy danh sách thành công' })
  async getLoginDevices(@Param('userId') userId: string) {
    return this.authService.getLoginHistory(userId);
  }

  @Get('login-history')
  @ApiOperation({ summary: 'Lấy tất cả lịch sử đăng nhập (tất cả users)' })
  @ApiResponse({ status: 200, description: 'Lấy danh sách thành công' })
  async getAllLoginHistory() {
    return this.authService.getAllLoginHistory();
  }
}
