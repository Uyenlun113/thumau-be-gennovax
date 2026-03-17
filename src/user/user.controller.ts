import { Controller, Get, Post, Put, Delete, Body, Param, Query, UseGuards, Request } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiParam, ApiQuery, ApiBearerAuth } from '@nestjs/swagger';
import { UserService } from './user.service';
import { CreateUserDto } from './dto/create-user.dto';
import { UpdateUserDto } from './dto/update-user.dto';
import { UpdateLocationDto } from './dto/update-location.dto';
import { ChangePasswordDto } from './dto/change-password.dto';
import { UpdateProfileDto } from './dto/update-profile.dto';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { PermissionsGuard } from '../auth/guards/permissions.guard';
import { Permissions } from '../auth/decorators/permissions.decorator';
import { Permission } from '../role/schemas/role.schema';

@ApiTags('users')
@ApiBearerAuth('JWT-auth')
@Controller('users')
@UseGuards(JwtAuthGuard, PermissionsGuard)
export class UserController {
  constructor(private readonly userService: UserService) {}

  @Post()
  @Permissions(Permission.EMPLOYEE_CREATE)
  @ApiOperation({ summary: 'Tạo người dùng mới' })
  @ApiResponse({ status: 201, description: 'Người dùng đã được tạo thành công' })
  @ApiResponse({ status: 400, description: 'Dữ liệu không hợp lệ' })
  create(@Body() data: CreateUserDto) {
    return this.userService.create(data);
  }

  @Get()
  @Permissions(Permission.EMPLOYEE_VIEW)
  @ApiOperation({ summary: 'Lấy danh sách người dùng với phân trang và tìm kiếm' })
  @ApiQuery({ name: 'role', description: 'Lọc theo vai trò (ID)', required: false })
  @ApiQuery({ name: 'status', description: 'Lọc theo trạng thái hoạt động', required: false, type: Boolean })
  @ApiQuery({ name: 'search', description: 'Tìm kiếm theo tên, mã nhân viên, email', required: false })
  @ApiQuery({ name: 'page', description: 'Số trang (bắt đầu từ 1)', required: false, type: Number })
  @ApiQuery({ name: 'limit', description: 'Số lượng mỗi trang', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Danh sách người dùng' })
  findAll(
    @Query('role') role?: string,
    @Query('status') status?: string,
    @Query('search') search?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
  ) {
    const pageNum = page ? parseInt(page, 10) : undefined;
    const limitNum = limit ? parseInt(limit, 10) : undefined;
    const statusBool = status === 'true' ? true : status === 'false' ? false : undefined;
    
    return this.userService.findAllWithPagination({
      role,
      status: statusBool,
      search,
      page: pageNum,
      limit: limitNum,
    });
  }

  @Get('nearest')
  @Permissions(Permission.EMPLOYEE_VIEW)
  @ApiOperation({ summary: 'Tìm nhân viên gần nhất' })
  @ApiQuery({ name: 'lat', description: 'Latitude', example: '10.762622' })
  @ApiQuery({ name: 'lng', description: 'Longitude', example: '106.660172' })
  @ApiResponse({ status: 200, description: 'Thông tin nhân viên gần nhất' })
  findNearestStaff(@Query('lat') lat: string, @Query('lng') lng: string) {
    return this.userService.findNearestStaff({
      lat: parseFloat(lat),
      lng: parseFloat(lng),
    });
  }

  @Get(':id')
  @Permissions(Permission.EMPLOYEE_DETAIL_VIEW)
  @ApiOperation({ summary: 'Lấy thông tin người dùng theo ID' })
  @ApiParam({ name: 'id', description: 'ID người dùng' })
  @ApiResponse({ status: 200, description: 'Thông tin người dùng' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy người dùng' })
  findOne(@Param('id') id: string) {
    return this.userService.findOne(id);
  }

  @Put(':id')
  @Permissions(Permission.EMPLOYEE_UPDATE)
  @ApiOperation({ summary: 'Cập nhật thông tin người dùng' })
  @ApiParam({ name: 'id', description: 'ID người dùng' })
  @ApiResponse({ status: 200, description: 'Người dùng đã được cập nhật' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy người dùng' })
  update(@Param('id') id: string, @Body() data: UpdateUserDto) {
    return this.userService.update(id, data);
  }

  @Put(':id/location')
  @ApiOperation({ summary: 'Cập nhật vị trí người dùng' })
  @ApiParam({ name: 'id', description: 'ID người dùng' })
  @ApiResponse({ status: 200, description: 'Vị trí đã được cập nhật' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy người dùng' })
  updateLocation(@Param('id') id: string, @Body() location: UpdateLocationDto) {
    return this.userService.updateLocation(id, location);
  }

  @Delete(':id')
  @Permissions(Permission.EMPLOYEE_DELETE)
  @ApiOperation({ summary: 'Xóa người dùng' })
  @ApiParam({ name: 'id', description: 'ID người dùng' })
  @ApiResponse({ status: 200, description: 'Người dùng đã được xóa' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy người dùng' })
  delete(@Param('id') id: string) {
    return this.userService.delete(id);
  }

  @Get('profile/me')
  @ApiOperation({ summary: 'Lấy thông tin cá nhân' })
  @ApiResponse({ status: 200, description: 'Thông tin cá nhân' })
  getProfile(@Request() req) {
    return this.userService.getProfile(req.user._id.toString());
  }

  @Put('profile/me')
  @ApiOperation({ summary: 'Cập nhật thông tin cá nhân' })
  @ApiResponse({ status: 200, description: 'Cập nhật thành công' })
  updateProfile(@Request() req, @Body() dto: UpdateProfileDto) {
    return this.userService.updateProfile(req.user._id.toString(), dto);
  }

  @Post('profile/change-password')
  @ApiOperation({ summary: 'Đổi mật khẩu' })
  @ApiResponse({ status: 200, description: 'Đổi mật khẩu thành công' })
  @ApiResponse({ status: 400, description: 'Mật khẩu không hợp lệ' })
  @ApiResponse({ status: 401, description: 'Mật khẩu hiện tại không đúng' })
  changePassword(@Request() req, @Body() dto: ChangePasswordDto) {
    return this.userService.changePassword(req.user._id.toString(), dto);
  }
}
