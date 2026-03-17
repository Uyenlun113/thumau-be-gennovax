import { Controller, Get, Post, Put, Delete, Body, Param, Query, UseGuards } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiParam, ApiQuery, ApiBearerAuth } from '@nestjs/swagger';
import { ClinicService } from './clinic.service';
import { CreateClinicDto } from './dto/create-clinic.dto';
import { UpdateClinicDto } from './dto/update-clinic.dto';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { PermissionsGuard } from '../auth/guards/permissions.guard';
import { Permissions } from '../auth/decorators/permissions.decorator';
import { Permission } from '../role/schemas/role.schema';

@ApiTags('clinics')
@ApiBearerAuth('JWT-auth')
@Controller('clinics')
@UseGuards(JwtAuthGuard, PermissionsGuard)
export class ClinicController {
  constructor(private readonly clinicService: ClinicService) {}

  @Post()
  @Permissions(Permission.CLINIC_CREATE)
  @ApiOperation({ summary: 'Tạo phòng khám mới' })
  @ApiResponse({ status: 201, description: 'Phòng khám đã được tạo thành công' })
  @ApiResponse({ status: 400, description: 'Dữ liệu không hợp lệ' })
  create(@Body() data: CreateClinicDto) {
    return this.clinicService.create(data);
  }

  @Get()
  @Permissions(Permission.CLINIC_VIEW)
  @ApiOperation({ summary: 'Lấy danh sách phòng khám với phân trang và tìm kiếm' })
  @ApiQuery({ name: 'status', description: 'Lọc theo trạng thái hoạt động', required: false, type: Boolean })
  @ApiQuery({ name: 'search', description: 'Tìm kiếm theo tên, mã, địa chỉ, chuyên khoa', required: false })
  @ApiQuery({ name: 'page', description: 'Số trang (bắt đầu từ 1)', required: false, type: Number })
  @ApiQuery({ name: 'limit', description: 'Số lượng mỗi trang', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Danh sách phòng khám' })
  findAll(
    @Query('status') status?: string,
    @Query('search') search?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
  ) {
    const pageNum = page ? parseInt(page, 10) : undefined;
    const limitNum = limit ? parseInt(limit, 10) : undefined;
    const statusBool = status === 'true' ? true : status === 'false' ? false : undefined;
    
    return this.clinicService.findAllWithPagination({
      status: statusBool,
      search,
      page: pageNum,
      limit: limitNum,
    });
  }

  @Get('nearby')
  @Permissions(Permission.CLINIC_VIEW)
  @ApiOperation({ summary: 'Tìm phòng khám gần vị trí hiện tại' })
  @ApiQuery({ name: 'lat', description: 'Latitude', example: '10.762622' })
  @ApiQuery({ name: 'lng', description: 'Longitude', example: '106.660172' })
  @ApiQuery({ name: 'maxDistance', description: 'Khoảng cách tối đa (km)', required: false, example: '10' })
  @ApiResponse({ status: 200, description: 'Danh sách phòng khám gần đó' })
  findNearby(
    @Query('lat') lat: string,
    @Query('lng') lng: string,
    @Query('maxDistance') maxDistance?: string
  ) {
    return this.clinicService.findNearby(
      { lat: parseFloat(lat), lng: parseFloat(lng) },
      maxDistance ? parseFloat(maxDistance) : 10
    );
  }

  @Get('code/:maPhongKham')
  @Permissions(Permission.CLINIC_VIEW)
  @ApiOperation({ summary: 'Tìm phòng khám theo mã' })
  @ApiParam({ name: 'maPhongKham', description: 'Mã phòng khám', example: 'PK001' })
  @ApiResponse({ status: 200, description: 'Thông tin phòng khám' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phòng khám' })
  findByCode(@Param('maPhongKham') maPhongKham: string) {
    return this.clinicService.findByCode(maPhongKham);
  }

  @Get(':id')
  @Permissions(Permission.CLINIC_DETAIL_VIEW)
  @ApiOperation({ summary: 'Lấy thông tin phòng khám theo ID' })
  @ApiParam({ name: 'id', description: 'ID phòng khám' })
  @ApiResponse({ status: 200, description: 'Thông tin phòng khám' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phòng khám' })
  findOne(@Param('id') id: string) {
    return this.clinicService.findOne(id);
  }

  @Put(':id')
  @Permissions(Permission.CLINIC_UPDATE)
  @ApiOperation({ summary: 'Cập nhật thông tin phòng khám' })
  @ApiParam({ name: 'id', description: 'ID phòng khám' })
  @ApiResponse({ status: 200, description: 'Phòng khám đã được cập nhật' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phòng khám' })
  update(@Param('id') id: string, @Body() data: UpdateClinicDto) {
    return this.clinicService.update(id, data);
  }

  @Delete(':id')
  @Permissions(Permission.CLINIC_DELETE)
  @ApiOperation({ summary: 'Xóa phòng khám' })
  @ApiParam({ name: 'id', description: 'ID phòng khám' })
  @ApiResponse({ status: 200, description: 'Phòng khám đã được xóa' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phòng khám' })
  delete(@Param('id') id: string) {
    return this.clinicService.delete(id);
  }
}
