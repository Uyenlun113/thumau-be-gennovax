import { Body, Controller, Delete, Get, Param, Post, Put, Query, Res, UploadedFile, UseGuards, UseInterceptors } from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { ApiBearerAuth, ApiBody, ApiConsumes, ApiOperation, ApiParam, ApiQuery, ApiResponse, ApiTags } from '@nestjs/swagger';
import { Response } from 'express';
import { diskStorage } from 'multer';
import { extname } from 'path';
import { Permissions } from '../auth/decorators/permissions.decorator';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { PermissionsGuard } from '../auth/guards/permissions.guard';
import { Permission } from '../role/schemas/role.schema';
import { AdjustStockDto } from './dto/adjust-stock.dto';
import { ConfirmDeliveryDto } from './dto/confirm-delivery.dto';
import { CreateAllocationDto } from './dto/create-allocation.dto';
import { CreateSupplyDto } from './dto/create-supply.dto';
import { PrepareAllocationDto } from './dto/prepare-allocation.dto';
import { UpdateSupplyDto } from './dto/update-supply.dto';
import { SupplyService } from './supply.service';

@ApiTags('supply')
@ApiBearerAuth('JWT-auth')
@Controller('supply')
@UseGuards(JwtAuthGuard, PermissionsGuard)
export class SupplyController {
  constructor(private readonly supplyService: SupplyService) { }

  // ============ UPLOAD ẢNH ============

  @Post('upload-image')
  @Permissions(Permission.SUPPLY_CREATE, Permission.SUPPLY_UPDATE)
  @ApiOperation({ summary: 'Upload hình ảnh vật tư' })
  @ApiConsumes('multipart/form-data')
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        file: {
          type: 'string',
          format: 'binary',
        },
      },
    },
  })
  @ApiResponse({
    status: 200, description: 'Upload thành công', schema: {
      type: 'object',
      properties: {
        success: { type: 'boolean' },
        filename: { type: 'string' },
        path: { type: 'string' },
      },
    }
  })
  @ApiResponse({ status: 400, description: 'File không hợp lệ' })
  @UseInterceptors(
    FileInterceptor('file', {
      storage: diskStorage({
        destination: './uploads/supplies',
        filename: (_req, file, cb) => {
          const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
          const ext = extname(file.originalname);
          cb(null, `supply-${uniqueSuffix}${ext}`);
        },
      }),
    }),
  )
  uploadImage(@UploadedFile() file: Express.Multer.File) {
    return {
      success: true,
      filename: file.filename,
      path: `/uploads/supplies/${file.filename}`,
    };
  }

  @Post('allocations/:id/upload-delivery-image')
  @Permissions(Permission.ALLOCATION_UPDATE)
  @ApiOperation({ summary: 'Upload hình ảnh giao nhận' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp phát' })
  @ApiConsumes('multipart/form-data')
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        file: {
          type: 'string',
          format: 'binary',
        },
      },
    },
  })
  @ApiResponse({
    status: 200, description: 'Upload thành công', schema: {
      type: 'object',
      properties: {
        success: { type: 'boolean' },
        filename: { type: 'string' },
        path: { type: 'string' },
      },
    }
  })
  @ApiResponse({ status: 400, description: 'File không hợp lệ' })
  @UseInterceptors(
    FileInterceptor('file', {
      storage: diskStorage({
        destination: './uploads/deliveries',
        filename: (_req, file, cb) => {
          const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
          const ext = extname(file.originalname);
          cb(null, `delivery-${uniqueSuffix}${ext}`);
        },
      }),
    }),
  )
  uploadDeliveryImage(@UploadedFile() file: Express.Multer.File) {
    return {
      success: true,
      filename: file.filename,
      path: `/uploads/deliveries/${file.filename}`,
    };
  }

  // ============ QUẢN LÝ VẬT TƯ ============

  @Post()
  @ApiOperation({ summary: 'Tạo vật tư mới' })
  @ApiResponse({ status: 201, description: 'Vật tư đã được tạo thành công' })
  @ApiResponse({ status: 400, description: 'Dữ liệu không hợp lệ' })
  createSupply(@Body() createSupplyDto: CreateSupplyDto) {
    return this.supplyService.createSupply(createSupplyDto);
  }

  @Get()
  @ApiOperation({ summary: 'Lấy danh sách vật tư với phân trang và tìm kiếm' })
  @ApiQuery({ name: 'status', description: 'Lọc theo trạng thái', required: false, enum: ['BINH_THUONG', 'CAN_NHAP_THEM'] })
  @ApiQuery({ name: 'search', description: 'Tìm kiếm theo mã, tên, mô tả', required: false })
  @ApiQuery({ name: 'page', description: 'Số trang (bắt đầu từ 1)', required: false, type: Number })
  @ApiQuery({ name: 'limit', description: 'Số lượng mỗi trang', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Danh sách vật tư' })
  findAllSupplies(
    @Query('status') status?: string,
    @Query('search') search?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
  ) {
    const pageNum = page ? parseInt(page, 10) : undefined;
    const limitNum = limit ? parseInt(limit, 10) : undefined;

    return this.supplyService.findAllSuppliesWithPagination({
      status,
      search,
      page: pageNum,
      limit: limitNum,
    });
  }

  @Post(':id/adjust-stock')
  @ApiOperation({ summary: 'Điều chỉnh tồn kho vật tư' })
  @ApiParam({ name: 'id', description: 'ID vật tư' })
  @ApiResponse({ status: 200, description: 'Điều chỉnh kho thành công' })
  @ApiResponse({ status: 400, description: 'Số lượng không hợp lệ' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy vật tư' })
  adjustStock(@Param('id') id: string, @Body() adjustStockDto: AdjustStockDto) {
    return this.supplyService.adjustStock(id, adjustStockDto);
  }

  @Get(':id/history')
  @ApiOperation({ summary: 'Lấy lịch sử điều chỉnh kho của vật tư' })
  @ApiParam({ name: 'id', description: 'ID vật tư' })
  @ApiQuery({ name: 'startDate', description: 'Ngày bắt đầu (ISO format)', required: false })
  @ApiQuery({ name: 'endDate', description: 'Ngày kết thúc (ISO format)', required: false })
  @ApiResponse({ status: 200, description: 'Lịch sử điều chỉnh kho' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy vật tư' })
  getSupplyHistory(
    @Param('id') id: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
  ) {
    return this.supplyService.getSupplyHistory(id, { startDate, endDate });
  }

  @Get(':id')
  @ApiOperation({ summary: 'Lấy thông tin vật tư theo ID' })
  @ApiParam({ name: 'id', description: 'ID vật tư' })
  @ApiResponse({ status: 200, description: 'Thông tin vật tư' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy vật tư' })
  findSupplyById(@Param('id') id: string) {
    return this.supplyService.findSupplyById(id);
  }

  @Put(':id')
  @ApiOperation({ summary: 'Cập nhật thông tin vật tư' })
  @ApiParam({ name: 'id', description: 'ID vật tư' })
  @ApiResponse({ status: 200, description: 'Vật tư đã được cập nhật' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy vật tư' })
  updateSupply(@Param('id') id: string, @Body() updateSupplyDto: UpdateSupplyDto) {
    return this.supplyService.updateSupply(id, updateSupplyDto);
  }

  @Delete(':id')
  @ApiOperation({ summary: 'Xóa vật tư' })
  @ApiParam({ name: 'id', description: 'ID vật tư' })
  @ApiResponse({ status: 200, description: 'Vật tư đã được xóa' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy vật tư' })
  deleteSupply(@Param('id') id: string) {
    return this.supplyService.deleteSupply(id);
  }

  // ============ QUẢN LÝ PHIẾU CẤP PHÁT ============

  @Post('allocations')
  @ApiOperation({ summary: 'Tạo phiếu cấp phát vật tư' })
  @ApiResponse({ status: 201, description: 'Phiếu cấp phát đã được tạo thành công' })
  @ApiResponse({ status: 400, description: 'Dữ liệu không hợp lệ hoặc không đủ tồn kho' })
  createAllocation(@Body() createAllocationDto: CreateAllocationDto) {
    return this.supplyService.createAllocation(createAllocationDto);
  }

  @Get('allocations/list')
  @ApiOperation({ summary: 'Lấy danh sách phiếu cấp phát với phân trang và lọc' })
  @ApiQuery({ name: 'status', description: 'Lọc theo trạng thái', required: false, enum: ['CHO_CHUAN_BI', 'CHUAN_BI_HANG', 'DA_GIAO'] })
  @ApiQuery({ name: 'search', description: 'Tìm kiếm theo tên phòng khám', required: false })
  @ApiQuery({ name: 'page', description: 'Số trang (bắt đầu từ 1)', required: false, type: Number })
  @ApiQuery({ name: 'limit', description: 'Số lượng mỗi trang', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Danh sách phiếu cấp phát' })
  findAllAllocations(
    @Query('status') status?: string,
    @Query('search') search?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
  ) {
    return this.supplyService.findAllAllocations({
      status,
      search,
      page: page ? parseInt(page) : undefined,
      limit: limit ? parseInt(limit) : undefined,
    });
  }

  @Get('allocations/:id')
  @ApiOperation({ summary: 'Lấy thông tin phiếu cấp phát theo ID' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp phát' })
  @ApiResponse({ status: 200, description: 'Thông tin phiếu cấp phát' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phiếu cấp phát' })
  findAllocationById(@Param('id') id: string) {
    return this.supplyService.findAllocationById(id);
  }

  @Post('allocations/:id/prepare')
  @ApiOperation({ summary: 'Chuẩn bị hàng - Cập nhật hạn sử dụng và trừ vật tư khỏi kho' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp phát' })
  @ApiResponse({ status: 200, description: 'Chuẩn bị hàng thành công, vật tư đã được trừ khỏi kho' })
  @ApiResponse({ status: 400, description: 'Không đủ tồn kho hoặc trạng thái không hợp lệ' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phiếu cấp phát' })
  prepareAllocation(@Param('id') id: string, @Body() prepareAllocationDto: PrepareAllocationDto) {
    return this.supplyService.prepareAllocation(id, prepareAllocationDto);
  }

  @Put('allocations/:id')
  @ApiOperation({ summary: 'Cập nhật phiếu cấp phát (chỉ khi ở trạng thái chờ chuẩn bị)' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp phát' })
  @ApiResponse({ status: 200, description: 'Cập nhật phiếu thành công' })
  @ApiResponse({ status: 400, description: 'Không thể sửa phiếu hoặc không đủ tồn kho' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phiếu cấp phát' })
  updateAllocation(@Param('id') id: string, @Body() updateAllocationDto: CreateAllocationDto) {
    return this.supplyService.updateAllocation(id, updateAllocationDto);
  }

  @Post('allocations/:id/confirm-delivery')
  @ApiOperation({ summary: 'Xác nhận đã giao hàng' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp phát' })
  @ApiResponse({ status: 200, description: 'Xác nhận giao hàng thành công' })
  @ApiResponse({ status: 400, description: 'Trạng thái không hợp lệ' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phiếu cấp phát' })
  confirmDelivery(@Param('id') id: string, @Body() confirmDeliveryDto: ConfirmDeliveryDto) {
    return this.supplyService.confirmDelivery(id, confirmDeliveryDto);
  }

  @Delete('allocations/:id')
  @ApiOperation({ summary: 'Xóa phiếu cấp phát và hoàn vật tư về kho' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp phát' })
  @ApiResponse({ status: 200, description: 'Xóa phiếu thành công, vật tư đã được hoàn về kho' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phiếu cấp phát' })
  deleteAllocation(@Param('id') id: string) {
    return this.supplyService.deleteAllocation(id);
  }

  @Post('allocations/:id/mark-zalo-sent')
  @ApiOperation({ summary: 'Đánh dấu đã gửi tin nhắn Zalo' })
  @ApiParam({ name: 'id', description: 'ID phiếu cấp phát' })
  @ApiResponse({ status: 200, description: 'Đánh dấu thành công' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy phiếu cấp phát' })
  markZaloSent(@Param('id') id: string) {
    return this.supplyService.markZaloSent(id);
  }

  @Get('allocations/template/download')
  @ApiOperation({ summary: 'Tải file Excel mẫu cho nhập phiếu cấp phát' })
  @ApiResponse({ status: 200, description: 'File Excel mẫu' })
  async downloadTemplate(@Res() res: Response) {
    const { buffer, filename } = await this.supplyService.generateExcelTemplate();

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(buffer);
  }

  @Post('allocations/import/excel')
  @ApiOperation({ summary: 'Nhập phiếu cấp phát từ file Excel' })
  @ApiConsumes('multipart/form-data')
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        file: {
          type: 'string',
          format: 'binary',
        },
        nguoiTaoPhieu: {
          type: 'string',
        },
      },
    },
  })
  @ApiResponse({ status: 200, description: 'Nhập thành công' })
  @ApiResponse({ status: 400, description: 'File không hợp lệ hoặc dữ liệu sai' })
  @UseInterceptors(FileInterceptor('file'))
  async importFromExcel(
    @UploadedFile() file: Express.Multer.File,
    @Body('nguoiTaoPhieu') nguoiTaoPhieu: string,
  ) {
    return this.supplyService.importAllocationsFromExcel(file, nguoiTaoPhieu);
  }

  // ============ BÁO CÁO TỒN KHO ============

  @Get('reports/inventory')
  @ApiOperation({ summary: 'Báo cáo tồn kho tổng hợp' })
  @ApiQuery({ name: 'phongKham', description: 'Lọc theo phòng khám', required: false })
  @ApiQuery({ name: 'vatTu', description: 'Lọc theo vật tư', required: false })
  @ApiQuery({ name: 'startDate', description: 'Ngày bắt đầu (ISO format)', required: false })
  @ApiQuery({ name: 'endDate', description: 'Ngày kết thúc (ISO format)', required: false })
  @ApiResponse({ status: 200, description: 'Báo cáo tồn kho' })
  getInventoryReport(
    @Query('phongKham') phongKham?: string,
    @Query('vatTu') vatTu?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
  ) {
    return this.supplyService.getInventoryReport({
      phongKham,
      vatTu,
      startDate,
      endDate,
    });
  }

  @Get('reports/allocation-detail')
  @ApiOperation({ summary: 'Báo cáo chi tiết theo phiếu cấp' })
  @ApiQuery({ name: 'phongKham', description: 'Lọc theo phòng khám', required: false })
  @ApiQuery({ name: 'search', description: 'Tìm kiếm theo mã phiếu, vật tư', required: false })
  @ApiQuery({ name: 'startDate', description: 'Ngày bắt đầu (ISO format)', required: false })
  @ApiQuery({ name: 'endDate', description: 'Ngày kết thúc (ISO format)', required: false })
  @ApiQuery({ name: 'page', description: 'Số trang (bắt đầu từ 1)', required: false, type: Number })
  @ApiQuery({ name: 'limit', description: 'Số lượng mỗi trang', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Báo cáo chi tiết phiếu cấp' })
  getAllocationDetailReport(
    @Query('phongKham') phongKham?: string,
    @Query('search') search?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
  ) {
    return this.supplyService.getAllocationDetailReport({
      phongKham,
      search,
      startDate,
      endDate,
      page: page ? parseInt(page) : undefined,
      limit: limit ? parseInt(limit) : undefined,
    });
  }

  // ============ NHẬP SỐ LƯỢNG MẪU NHẬN VỀ ============

  @Get('allocations/sample-return/history')
  @ApiOperation({ summary: 'Lấy lịch sử nhập mẫu nhận về' })
  @ApiQuery({ name: 'phongKham', description: 'Lọc theo phòng khám', required: false })
  @ApiQuery({ name: 'vatTu', description: 'Lọc theo vật tư', required: false })
  @ApiQuery({ name: 'startDate', description: 'Ngày bắt đầu (ISO format)', required: false })
  @ApiQuery({ name: 'endDate', description: 'Ngày kết thúc (ISO format)', required: false })
  @ApiQuery({ name: 'page', description: 'Số trang (bắt đầu từ 1)', required: false, type: Number })
  @ApiQuery({ name: 'limit', description: 'Số lượng mỗi trang', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Lịch sử nhập mẫu' })
  getSampleReturnHistory(
    @Query('phongKham') phongKham?: string,
    @Query('vatTu') vatTu?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
  ) {
    return this.supplyService.getSampleReturnHistory({
      phongKham,
      vatTu,
      startDate,
      endDate,
      page: page ? parseInt(page) : undefined,
      limit: limit ? parseInt(limit) : undefined,
    });
  }

  @Get('allocations/sample-return/template/download')
  @ApiOperation({ summary: 'Tải file Excel mẫu cho nhập số lượng mẫu nhận về' })
  @ApiResponse({ status: 200, description: 'File Excel mẫu' })
  async downloadSampleReturnTemplate(@Res() res: Response) {
    const { buffer, filename } = await this.supplyService.generateSampleReturnTemplate();

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(buffer);
  }

  @Post('allocations/sample-return/import/excel')
  @ApiOperation({ summary: 'Nhập số lượng mẫu nhận về từ file Excel' })
  @ApiConsumes('multipart/form-data')
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        file: {
          type: 'string',
          format: 'binary',
        },
        nguoiNhap: {
          type: 'string',
        },
      },
    },
  })
  @ApiResponse({ status: 200, description: 'Nhập thành công' })
  @ApiResponse({ status: 400, description: 'File không hợp lệ hoặc dữ liệu sai' })
  @UseInterceptors(FileInterceptor('file'))
  async importSampleReturn(
    @UploadedFile() file: Express.Multer.File,
    @Body('nguoiNhap') nguoiNhap: string,
  ) {
    return this.supplyService.importSampleReturnFromExcel(file, nguoiNhap);
  }

  @Delete('allocations/sample-return/history/:id')
  @ApiOperation({ summary: 'Xóa bản ghi lịch sử nhập mẫu nhận về' })
  @ApiResponse({ status: 200, description: 'Xóa thành công' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy bản ghi' })
  async deleteSampleReturnHistory(@Param('id') id: string) {
    return this.supplyService.deleteSampleReturnHistory(id);
  }

 
}
