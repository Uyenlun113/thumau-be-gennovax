import { Controller, Get, Post, Put, Delete, Body, Param, Query, UseGuards } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiParam, ApiQuery, ApiBearerAuth } from '@nestjs/swagger';
import { OrderService } from './order.service';
import { CreateOrderDto } from './dto/create-order.dto';
import { UpdateOrderDto } from './dto/update-order.dto';
import { AssignOrderDto } from './dto/assign-order.dto';
import { UpdateOrderStatusDto } from './dto/update-status.dto';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { PermissionsGuard } from '../auth/guards/permissions.guard';
import { Permissions } from '../auth/decorators/permissions.decorator';
import { Permission } from '../role/schemas/role.schema';

@ApiTags('orders')
@ApiBearerAuth('JWT-auth')
@Controller('orders')
@UseGuards(JwtAuthGuard, PermissionsGuard)
export class OrderController {
  constructor(private readonly orderService: OrderService) {}

  @Post()
  @Permissions(Permission.ORDER_CREATE)
  @ApiOperation({ summary: 'Tạo phiếu cấp mới' })
  @ApiResponse({ status: 201, description: 'Phiếu cấp đã được tạo thành công' })
  @ApiResponse({ status: 400, description: 'Dữ liệu không hợp lệ' })
  create(@Body() data: CreateOrderDto) {
    return this.orderService.create(data);
  }

  @Get()
  @Permissions(Permission.ORDER_VIEW)
  @ApiOperation({ summary: 'Lấy danh sách phiếu cấp với phân trang và tìm kiếm' })
  @ApiQuery({ name: 'status', description: 'Lọc theo trạng thái', required: false, enum: ['CHO_DIEU_PHOI', 'CHO_NHAN_LENH', 'DANG_THUC_HIEN', 'HOAN_THANH', 'DA_HUY'] })
  @ApiQuery({ name: 'search', description: 'Tìm kiếm theo mã phiếu', required: false })
  @ApiQuery({ name: 'page', description: 'Số trang (bắt đầu từ 1)', required: false, type: Number })
  @ApiQuery({ name: 'limit', description: 'Số lượng mỗi trang', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Danh sách phiếu cấp' })
  findAll(
    @Query('status') status?: string,
    @Query('search') search?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
  ) {
    const pageNum = page ? parseInt(page, 10) : undefined;
    const limitNum = limit ? parseInt(limit, 10) : undefined;
    
    return this.orderService.findAllWithPagination({
      status,
      search,
      page: pageNum,
      limit: limitNum,
    });
  }

  @Get('stats/summary')
  @Permissions(Permission.DASHBOARD_VIEW)
  @ApiOperation({ summary: 'Lấy thống kê tổng quan phiếu cấp' })
  @ApiResponse({ status: 200, description: 'Thống kê phiếu cấp' })
  getStatsSummary() {
    return this.orderService.getStatsSummary();
  }

  @Get(':id')
  @Permissions(Permission.ORDER_DETAIL_VIEW)
  @ApiOperation({ summary: 'Lấy thông tin phiếu cấp theo ID' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp' })
  @ApiResponse({ status: 200, description: 'Thông tin phiếu cấp' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phiếu cấp' })
  findOne(@Param('id') id: string) {
    return this.orderService.findOne(id);
  }

  @Put(':id')
  @Permissions(Permission.ORDER_UPDATE)
  @ApiOperation({ summary: 'Cập nhật thông tin phiếu cấp' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp' })
  @ApiResponse({ status: 200, description: 'Phiếu cấp đã được cập nhật' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phiếu cấp' })
  update(@Param('id') id: string, @Body() data: UpdateOrderDto) {
    return this.orderService.update(id, data);
  }

  @Put(':id/assign')
  @Permissions(Permission.ORDER_ASSIGN)
  @ApiOperation({ summary: 'Phân công người thực hiện' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp' })
  @ApiResponse({ status: 200, description: 'Đã phân công thành công' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phiếu cấp' })
  assignOrder(@Param('id') id: string, @Body() data: AssignOrderDto) {
    return this.orderService.assignOrder(id, data.nguoiThucHien);
  }

  @Put(':id/status')
  @Permissions(Permission.ORDER_UPDATE)
  @ApiOperation({ summary: 'Cập nhật trạng thái phiếu cấp' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp' })
  @ApiResponse({ status: 200, description: 'Trạng thái đã được cập nhật' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phiếu cấp' })
  updateStatus(@Param('id') id: string, @Body() data: UpdateOrderStatusDto) {
    return this.orderService.updateStatus(id, data.trangThai, data.toaDo);
  }

  @Delete(':id')
  @Permissions(Permission.ORDER_DELETE)
  @ApiOperation({ summary: 'Xóa phiếu cấp' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp' })
  @ApiResponse({ status: 200, description: 'Phiếu cấp đã được xóa' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phiếu cấp' })
  delete(@Param('id') id: string) {
    return this.orderService.delete(id);
  }

  @Get(':id/messages')
  @ApiOperation({ summary: 'Lấy danh sách tin nhắn của lệnh' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp' })
  @ApiResponse({ status: 200, description: 'Danh sách tin nhắn' })
  getMessages(@Param('id') id: string) {
    return this.orderService.getMessages(id);
  }

  @Post(':id/messages')
  @ApiOperation({ summary: 'Gửi tin nhắn trong lệnh' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp' })
  @ApiResponse({ status: 201, description: 'Tin nhắn đã được gửi' })
  sendMessage(@Param('id') id: string, @Body() data: { userId: string; message: string }) {
    return this.orderService.sendMessage(id, data.userId, data.message);
  }
}
