import { Body, Controller, Delete, Get, Param, Post, Put, Query, UseGuards } from '@nestjs/common';
import { ApiBearerAuth, ApiOperation, ApiParam, ApiQuery, ApiResponse, ApiTags } from '@nestjs/swagger';
import { Permissions } from '../auth/decorators/permissions.decorator';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { PermissionsGuard } from '../auth/guards/permissions.guard';
import { CreateRoleDto } from './dto/create-role.dto';
import { UpdateRoleDto } from './dto/update-role.dto';
import { RoleService } from './role.service';
import { Permission } from './schemas/role.schema';

@ApiTags('roles')
@ApiBearerAuth('JWT-auth')
@Controller('roles')
@UseGuards(JwtAuthGuard, PermissionsGuard)
export class RoleController {
  constructor(private readonly roleService: RoleService) { }

  @Post()
  @Permissions(Permission.ROLE_CREATE)
  @ApiOperation({ summary: 'Tạo vai trò mới' })
  @ApiResponse({ status: 201, description: 'Vai trò đã được tạo thành công' })
  @ApiResponse({ status: 400, description: 'Dữ liệu không hợp lệ' })
  create(@Body() data: CreateRoleDto) {
    return this.roleService.create(data);
  }

  @Post('seed')
  @Permissions(Permission.PERMISSION_MANAGE)
  @ApiOperation({ summary: 'Seed dữ liệu vai trò mặc định' })
  @ApiResponse({ status: 201, description: 'Đã seed dữ liệu vai trò thành công' })
  seedRoles() {
    return this.roleService.seedDefaultRoles();
  }

  @Get()
  @Permissions(Permission.ROLE_VIEW)
  @ApiOperation({ summary: 'Lấy danh sách vai trò với phân trang và tìm kiếm' })
  @ApiQuery({ name: 'search', description: 'Tìm kiếm theo tên vai trò', required: false })
  @ApiQuery({ name: 'page', description: 'Số trang (bắt đầu từ 1)', required: false, type: Number })
  @ApiQuery({ name: 'limit', description: 'Số lượng mỗi trang', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Danh sách vai trò' })
  findAll(
    @Query('search') search?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
  ) {
    const pageNum = page ? parseInt(page, 10) : undefined;
    const limitNum = limit ? parseInt(limit, 10) : undefined;

    return this.roleService.findAllWithPagination({
      search,
      page: pageNum,
      limit: limitNum,
    });
  }

  @Get('permissions/list')
  @Permissions(Permission.PERMISSION_VIEW)
  @ApiOperation({ summary: 'Lấy danh sách tất cả permissions' })
  @ApiResponse({ status: 200, description: 'Danh sách permissions' })
  getAllPermissions() {
    return this.roleService.getAllPermissions();
  }

  @Get(':id')
  @Permissions(Permission.ROLE_VIEW)
  @ApiOperation({ summary: 'Lấy thông tin vai trò theo ID' })
  @ApiParam({ name: 'id', description: 'ID vai trò' })
  @ApiResponse({ status: 200, description: 'Thông tin vai trò' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy vai trò' })
  findOne(@Param('id') id: string) {
    return this.roleService.findOne(id);
  }

  @Put(':id')
  @Permissions(Permission.ROLE_UPDATE)
  @ApiOperation({ summary: 'Cập nhật thông tin vai trò' })
  @ApiParam({ name: 'id', description: 'ID vai trò' })
  @ApiResponse({ status: 200, description: 'Vai trò đã được cập nhật' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy vai trò' })
  update(@Param('id') id: string, @Body() data: UpdateRoleDto) {
    return this.roleService.update(id, data);
  }

  @Delete(':id')
  @Permissions(Permission.ROLE_DELETE)
  @ApiOperation({ summary: 'Xóa vai trò' })
  @ApiParam({ name: 'id', description: 'ID vai trò' })
  @ApiResponse({ status: 200, description: 'Vai trò đã được xóa' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy vai trò' })
  delete(@Param('id') id: string) {
    return this.roleService.delete(id);
  }
}
