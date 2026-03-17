import { Body, Controller, Delete, Get, Param, Post, Put, Query, Req, Res, UploadedFiles, UseGuards, UseInterceptors } from '@nestjs/common';
import { FilesInterceptor } from '@nestjs/platform-express';
import { ApiBearerAuth, ApiConsumes, ApiOperation, ApiParam, ApiQuery, ApiResponse, ApiTags } from '@nestjs/swagger';
import { Response } from 'express';
import { diskStorage } from 'multer';
import { extname } from 'path';
import { Permissions } from '../auth/decorators/permissions.decorator';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { PermissionsGuard } from '../auth/guards/permissions.guard';
import { Permission } from '../role/schemas/role.schema';
import { AssignStaffDto } from './dto/assign-staff.dto';
import { CreateSampleCollectionDto } from './dto/create-sample-collection.dto';
import { CompleteVerificationDto } from './dto/complete-verification.dto';
import { UpdateSampleCollectionDto } from './dto/update-sample-collection.dto';
import { UpdateSampleCollectionStatusDto } from './dto/update-status.dto';
import { SampleCollectionService } from './sample-collection.service';

@ApiTags('sample-collections')
@ApiBearerAuth('JWT-auth')
@Controller('sample-collections')
@UseGuards(JwtAuthGuard, PermissionsGuard)
export class SampleCollectionController {
  constructor(private readonly sampleCollectionService: SampleCollectionService) { }

  @Post()
  @Permissions(Permission.ORDER_CREATE)
  @ApiOperation({ summary: 'Tạo lệnh nhận mẫu mới' })
  @ApiResponse({ status: 201, description: 'Lệnh nhận mẫu đã được tạo thành công' })
  @ApiResponse({ status: 400, description: 'Dữ liệu không hợp lệ' })
  create(@Body() data: CreateSampleCollectionDto) {
    return this.sampleCollectionService.create(data);
  }

  @Get('count-by-status')
  @ApiOperation({ summary: 'Đếm số lượng lệnh nhận mẫu theo từng trạng thái' })
  @ApiQuery({ name: 'search', description: 'Tìm kiếm theo mã lệnh hoặc nội dung công việc', required: false })
  @ApiQuery({ name: 'employeeId', description: 'Lọc theo nhân viên thực hiện', required: false })
  @ApiQuery({ name: 'clinicId', description: 'Lọc theo phòng khám', required: false })
  @ApiQuery({ name: 'startDate', description: 'Từ ngày (ISO format)', required: false })
  @ApiQuery({ name: 'endDate', description: 'Đến ngày (ISO format)', required: false })
  @ApiResponse({ status: 200, description: 'Số lượng lệnh theo từng trạng thái' })
  countByStatus(
    @Query('search') search?: string,
    @Query('employeeId') employeeId?: string,
    @Query('clinicId') clinicId?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
    @Req() req?: any,
  ) {
    const user = req?.user;

    return this.sampleCollectionService.countByStatus({
      search,
      employeeId,
      clinicId,
      startDate,
      endDate,
      currentUser: user,
    });
  }



  @Get()
  // @Permissions(Permission.ORDER_VIEW) // Temporarily disabled for testing
  @ApiOperation({ summary: 'Lấy danh sách lệnh nhận mẫu với phân trang và tìm kiếm' })
  @ApiQuery({ name: 'status', description: 'Lọc theo trạng thái', required: false, enum: ['CHO_DIEU_PHOI', 'CHO_NHAN_LENH', 'DANG_THUC_HIEN', 'HOAN_THANH', 'DA_HUY'] })
  @ApiQuery({ name: 'search', description: 'Tìm kiếm theo mã lệnh hoặc nội dung công việc', required: false })
  @ApiQuery({ name: 'employeeId', description: 'Lọc theo nhân viên thực hiện', required: false })
  @ApiQuery({ name: 'clinicId', description: 'Lọc theo phòng khám', required: false })
  @ApiQuery({ name: 'startDate', description: 'Từ ngày (ISO format)', required: false })
  @ApiQuery({ name: 'endDate', description: 'Đến ngày (ISO format)', required: false })
  @ApiQuery({ name: 'page', description: 'Số trang (bắt đầu từ 1)', required: false, type: Number })
  @ApiQuery({ name: 'limit', description: 'Số lượng mỗi trang', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Danh sách lệnh nhận mẫu' })
  findAll(
    @Query('status') status?: string,
    @Query('search') search?: string,
    @Query('employeeId') employeeId?: string,
    @Query('clinicId') clinicId?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Req() req?: any,
  ) {
    const pageNum = page ? parseInt(page, 10) : undefined;
    const limitNum = limit ? parseInt(limit, 10) : undefined;

    // Lấy user từ request (được set bởi JwtAuthGuard)
    const user = req?.user;

    return this.sampleCollectionService.findAllWithPagination({
      status,
      search,
      employeeId,
      clinicId,
      startDate,
      endDate,
      page: pageNum,
      limit: limitNum,
      currentUser: user,
    });
  }

  @Get('export/excel')
  @ApiOperation({ summary: 'Xuất danh sách lệnh nhận mẫu ra Excel' })
  @ApiResponse({ status: 200, description: 'File Excel' })
  @ApiQuery({ name: 'status', required: false, description: 'Lọc theo trạng thái' })
  @ApiQuery({ name: 'search', required: false, description: 'Tìm kiếm theo mã lệnh hoặc nội dung' })
  @ApiQuery({ name: 'employeeId', required: false, description: 'Lọc theo nhân viên' })
  @ApiQuery({ name: 'clinicId', required: false, description: 'Lọc theo phòng khám' })
  @ApiQuery({ name: 'startDate', required: false, description: 'Ngày bắt đầu (YYYY-MM-DD)' })
  @ApiQuery({ name: 'endDate', required: false, description: 'Ngày kết thúc (YYYY-MM-DD)' })
  async exportExcel(
    @Query('status') status?: string,
    @Query('search') search?: string,
    @Query('employeeId') employeeId?: string,
    @Query('clinicId') clinicId?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
    @Req() req?: any,
    @Res() res?: Response,
  ) {
    const buffer = await this.sampleCollectionService.exportToExcel({
      status,
      search,
      employeeId,
      clinicId,
      startDate,
      endDate,
      currentUser: req.user,
    });

    const filename = `lenh-thu-mau-${Date.now()}.xlsx`;

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Length', buffer.length);

    res.send(buffer);
  }

  @Get('stats/summary')
  @ApiOperation({ summary: 'Lấy thống kê tổng quan lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Thống kê lệnh nhận mẫu' })
  getStatsSummary() {
    return this.sampleCollectionService.getStatsSummary();
  }

  @Get('stats/dashboard')
  @ApiOperation({ summary: 'Lấy thống kê cho Dashboard với filter' })
  @ApiQuery({ name: 'status', description: 'Lọc theo trạng thái', required: false })
  @ApiQuery({ name: 'employeeId', description: 'Lọc theo nhân viên', required: false })
  @ApiQuery({ name: 'startDate', description: 'Từ ngày (ISO format)', required: false })
  @ApiQuery({ name: 'endDate', description: 'Đến ngày (ISO format)', required: false })
  @ApiResponse({ status: 200, description: 'Thống kê Dashboard' })
  getDashboardStats(
    @Query('status') status?: string,
    @Query('employeeId') employeeId?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
  ) {
    return this.sampleCollectionService.getDashboardStats({
      status,
      employeeId,
      startDate,
      endDate,
    });
  }

  @Get('history/all')
  @ApiOperation({ summary: 'Lấy tất cả lịch sử tiến trình (tất cả lệnh)' })
  @ApiResponse({ status: 200, description: 'Danh sách lịch sử tiến trình' })
  getAllHistory() {
    return this.sampleCollectionService.getAllHistory();
  }

  @Get('staff/:staffId')
  @ApiOperation({ summary: 'Lấy danh sách lệnh nhận mẫu theo nhân viên' })
  @ApiParam({ name: 'staffId', description: 'ID nhân viên' })
  @ApiResponse({ status: 200, description: 'Danh sách lệnh nhận mẫu của nhân viên' })
  findByStaff(@Param('staffId') staffId: string) {
    return this.sampleCollectionService.findByStaff(staffId);
  }

  @Get('clinic/:clinicId')
  @ApiOperation({ summary: 'Lấy danh sách lệnh nhận mẫu theo phòng khám' })
  @ApiParam({ name: 'clinicId', description: 'ID phòng khám' })
  @ApiResponse({ status: 200, description: 'Danh sách lệnh nhận mẫu của phòng khám' })
  findByClinic(@Param('clinicId') clinicId: string) {
    return this.sampleCollectionService.findByClinic(clinicId);
  }

  @Get('code/:maLenh')
  @ApiOperation({ summary: 'Tìm lệnh nhận mẫu theo mã' })
  @ApiParam({ name: 'maLenh', description: 'Mã lệnh', example: 'ML001' })
  @ApiResponse({ status: 200, description: 'Thông tin lệnh nhận mẫu' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy lệnh nhận mẫu' })
  findByCode(@Param('maLenh') maLenh: string) {
    return this.sampleCollectionService.findByCode(maLenh);
  }

  @Get(':id/history')
  @ApiOperation({ summary: 'Lấy lịch sử tiến trình lệnh nhận mẫu' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Lịch sử tiến trình' })
  getHistory(@Param('id') id: string) {
    return this.sampleCollectionService.getHistory(id);
  }

  @Get(':id')
  @Permissions(Permission.ORDER_DETAIL_VIEW)
  @ApiOperation({ summary: 'Lấy thông tin lệnh nhận mẫu theo ID' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Thông tin lệnh nhận mẫu' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy lệnh nhận mẫu' })
  findOne(@Param('id') id: string) {
    return this.sampleCollectionService.findOne(id);
  }

  @Put(':id')
  @Permissions(Permission.ORDER_UPDATE)
  @ApiOperation({ summary: 'Cập nhật thông tin lệnh nhận mẫu' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Lệnh nhận mẫu đã được cập nhật' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy lệnh nhận mẫu' })
  update(@Param('id') id: string, @Body() data: UpdateSampleCollectionDto) {
    return this.sampleCollectionService.update(id, data);
  }

  @Put(':id/assign')
  @Permissions(Permission.ORDER_ASSIGN)
  @ApiOperation({ summary: 'Phân công nhân viên thực hiện' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Đã phân công thành công' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy lệnh nhận mẫu' })
  assignStaff(@Param('id') id: string, @Body() data: AssignStaffDto) {
    return this.sampleCollectionService.assignStaff(id, data.nhanVienThucHien);
  }

  @Put(':id/status')
  @Permissions(Permission.ORDER_UPDATE)
  @ApiOperation({ summary: 'Cập nhật trạng thái lệnh nhận mẫu' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Trạng thái đã được cập nhật' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy lệnh nhận mẫu' })
  async updateStatus(
    @Param('id') id: string,
    @Body() data: UpdateSampleCollectionStatusDto
  ) {
    const { trangThai, ...additionalData } = data;
    const result = await this.sampleCollectionService.updateStatus(id, trangThai, additionalData);

    // Trả về kết quả kèm thông tin email status nếu có
    const response: any = result.toObject();
    if ((additionalData as any).emailStatus) {
      response.emailStatus = (additionalData as any).emailStatus;
    }

    return response;
  }

  @Post(':id/resend-email')
  @Permissions(Permission.ORDER_UPDATE)
  @ApiOperation({ summary: 'Gửi lại email thông báo' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Email đã được gửi lại' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy lệnh nhận mẫu' })
  async resendEmail(@Param('id') id: string) {
    return this.sampleCollectionService.resendCompletionEmail(id);
  }

  @Delete(':id')
  @Permissions(Permission.ORDER_DELETE)
  @ApiOperation({ summary: 'Xóa lệnh nhận mẫu' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Lệnh nhận mẫu đã được xóa' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy lệnh nhận mẫu' })
  delete(@Param('id') id: string) {
    return this.sampleCollectionService.delete(id);
  }

  @Post(':id/upload-images')
  @ApiOperation({ summary: 'Upload hình ảnh hoàn thành' })
  @ApiConsumes('multipart/form-data')
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiQuery({ name: 'saveToDb', description: 'Có lưu vào database không (true/false)', required: false })
  @ApiResponse({ status: 200, description: 'Upload thành công' })
  @UseInterceptors(
    FilesInterceptor('images', 10, {
      storage: diskStorage({
        destination: './uploads/sample-collections',
        filename: (req, file, callback) => {
          const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
          const ext = extname(file.originalname);
          callback(null, `${file.fieldname}-${uniqueSuffix}${ext}`);
        },
      }),
      fileFilter: (req, file, callback) => {
        if (!file.originalname.match(/\.(jpg|jpeg|png|gif)$/)) {
          return callback(new Error('Chỉ chấp nhận file ảnh!'), false);
        }
        callback(null, true);
      },
      limits: {
        fileSize: 5 * 1024 * 1024, // 5MB
      },
    }),
  )
  async uploadImages(
    @Param('id') id: string,
    @UploadedFiles() files: Array<any>,
    @Query('saveToDb') saveToDb?: string,
    @Query('type') type?: string, // 'completion' or 'verification'
  ) {
    const imagePaths = files.map(file => `/uploads/sample-collections/${file.filename}`);

    // Nếu saveToDb = 'true', lưu vào database (cho bước hoàn thành standard)
    // Nếu không, chỉ trả về đường dẫn (cho bước verification bus station)
    if (saveToDb === 'true') {
      // Get existing order to append to existing images instead of replacing
      const existingOrder = await this.sampleCollectionService.findOne(id);
      const existingImages = existingOrder?.anhHoanThanh || [];
      const combinedImages = [...existingImages, ...imagePaths];
      
      return this.sampleCollectionService.update(id, {
        anhHoanThanh: combinedImages,
      });
    }

    // Trả về field name phù hợp với loại ảnh
    const fieldName = type === 'verification' ? 'anhHoanThanhKiemTra' : 'anhHoanThanh';
    
    return {
      [fieldName]: imagePaths,
      message: 'Upload thành công',
    };
  }

  @Post('auto-create')
  @ApiOperation({ summary: 'Kiểm tra và tự động tạo lệnh thu mẫu theo cấu hình phòng khám' })
  @ApiResponse({ status: 200, description: 'Kết quả tạo lệnh tự động' })
  async autoCreateOrders(@Body() body: { nguoiGiaoLenh: string }) {
    return this.sampleCollectionService.autoCreateOrders(body.nguoiGiaoLenh);
  }

  @Post(':id/complete-bus-station')
  @Permissions(Permission.ORDER_UPDATE)
  @ApiOperation({ summary: 'Hoàn thành lệnh nhận mẫu từ nhà xe (chỉ upload ảnh)' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Đã hoàn thành lệnh nhà xe' })
  @ApiResponse({ status: 400, description: 'Lệnh không phải là lệnh nhà xe' })
  async completeBusStationOrder(
    @Param('id') id: string,
    @Body() body: { anhHoanThanh: string[]; nguoiThucHien: string }
  ) {
    return this.sampleCollectionService.completeBusStationOrder(
      id,
      body.anhHoanThanh,
      body.nguoiThucHien
    );
  }

  @Post(':id/complete-verification-clinic-items')
  @Permissions(Permission.ORDER_UPDATE)
  @ApiOperation({ summary: 'Hoàn thành kiểm tra với nhiều phòng khám (cho lệnh nhà xe)' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Đã hoàn thành kiểm tra' })
  @ApiResponse({ status: 400, description: 'Lệnh không phải là lệnh nhà xe hoặc chưa hoàn thành' })
  async completeVerificationWithClinicItems(
    @Param('id') id: string,
    @Body() body: {
      phongKhamItems: Array<{
        phongKham: string;
        soTienCuocNhanMau: number;
        soTienShip: number;
        soTienGuiXe: number;
        anhHoanThanhKiemTra: string[];
      }>;
      nguoiThucHien: string;
    }
  ) {
    return this.sampleCollectionService.completeVerificationWithClinicItems(
      id,
      body.phongKhamItems,
      body.nguoiThucHien
    );
  }

  @Post(':id/complete-verification')
  @Permissions(Permission.ORDER_UPDATE)
  @ApiOperation({ summary: 'Hoàn thành kiểm tra cho lệnh thu mẫu thường' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Đã hoàn thành kiểm tra' })
  @ApiResponse({ status: 400, description: 'Lệnh chưa hoàn thành hoặc không hợp lệ' })
  async completeVerification(
    @Param('id') id: string,
    @Body() body: CompleteVerificationDto
  ) {
    return this.sampleCollectionService.completeVerification(
      id,
      body.anhHoanThanhKiemTra,
      body.nguoiThucHien
    );
  }

  @Put(':id/verification-images')
  @Permissions(Permission.ORDER_UPDATE)
  @ApiOperation({ summary: 'Cập nhật ảnh hoàn thành kiểm tra' })
  @ApiParam({ name: 'id', description: 'ID của lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Cập nhật ảnh thành công' })
  async updateVerificationImages(
    @Param('id') id: string,
    @Body() updateData: { phongKhamItems: any[] },
  ) {
    return this.sampleCollectionService.updateVerificationImages(id, updateData.phongKhamItems);
  }

  @Get(':id/messages')
  @ApiOperation({ summary: 'Lấy danh sách tin nhắn của lệnh' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 200, description: 'Danh sách tin nhắn' })
  getMessages(@Param('id') id: string) {
    return this.sampleCollectionService.getMessages(id);
  }

  @Post(':id/messages')
  @ApiOperation({ summary: 'Gửi tin nhắn trong lệnh' })
  @ApiParam({ name: 'id', description: 'ID lệnh nhận mẫu' })
  @ApiResponse({ status: 201, description: 'Tin nhắn đã được gửi' })
  sendMessage(@Param('id') id: string, @Body() data: { userId: string; message: string }) {
    return this.sampleCollectionService.sendMessage(id, data.userId, data.message);
  }


}
