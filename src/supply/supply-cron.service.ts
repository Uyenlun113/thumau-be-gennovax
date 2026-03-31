import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { SupplyService } from './supply.service';

@Injectable()
export class SupplyCronService {
  private readonly logger = new Logger(SupplyCronService.name);

  constructor(private readonly supplyService: SupplyService) {}

  // Chạy mỗi 5 phút để đồng bộ ca từ API bên thứ 3
  @Cron('*/5 * * * *', {
    name: 'sync-external-cases',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleSyncExternalCases() {
    this.logger.log('Starting auto-sync external cases...');

    try {
      // Không truyền nguoiThucHien cho cronjob tự động
      const result = await this.supplyService.syncExternalCasesAndDeductStock({
        nguoiThucHien: null, // Cronjob tự động không có người thực hiện
      });

      this.logger.log(
        `Auto-sync completed: ${result.processedCases} processed, ${result.skippedCases} skipped, ${result.errorCount} errors`,
      );
    } catch (error) {
      this.logger.error('Auto-sync failed:', error.message);
    }
  }
}
