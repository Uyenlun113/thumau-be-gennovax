import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { SupplyService } from './supply.service';

@ApiTags('supply-debug')
@Controller('supply-debug')
export class SupplyDebugController {
  constructor(private readonly supplyService: SupplyService) {}

  @Get('check-supplies-by-type/:serviceType')
  @ApiOperation({ summary: 'Debug: Kiểm tra vật tư theo loại' })
  @ApiResponse({ status: 200, description: 'Danh sách vật tư' })
  async debugCheckSuppliesByType(@Param('serviceType') serviceType: string) {
    return this.supplyService.debugCheckSuppliesByType(serviceType);
  }

  @Post('test-sync-one-case')
  @ApiOperation({ summary: 'Debug: Test sync 1 ca để xem log' })
  @ApiResponse({ status: 200, description: 'Kết quả test' })
  async testSyncOneCase(@Body() body: { caseCode: string; serviceType: string; serviceName: string; source: string; receivedAt?: string }) {
    return this.supplyService.testSyncOneCase(body);
  }

  @Get('check-allocation/:maPhieu')
  @ApiOperation({ summary: 'Debug: Kiểm tra phiếu cấp' })
  @ApiResponse({ status: 200, description: 'Thông tin phiếu cấp' })
  async checkAllocation(@Param('maPhieu') maPhieu: string) {
    return this.supplyService.debugCheckAllocation(maPhieu);
  }

  @Get('check-inventory/:clinicName')
  @ApiOperation({ summary: 'Debug: Kiểm tra báo cáo tồn kho cho phòng khám' })
  @ApiResponse({ status: 200, description: 'Chi tiết tồn kho' })
  async checkInventory(@Param('clinicName') clinicName: string) {
    return this.supplyService.debugInventoryForClinic(clinicName);
  }
}
