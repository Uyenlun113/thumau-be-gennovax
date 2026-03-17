import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { Clinic, ClinicSchema } from '../clinic/schemas/clinic.schema';
import { SupplyController } from './supply.controller';
import { SupplyService } from './supply.service';
import { Supply, SupplySchema } from './schemas/supply.schema';
import { SupplyAllocation, SupplyAllocationSchema } from './schemas/supply-allocation.schema';
import { SupplyHistory, SupplyHistorySchema } from './schemas/supply-history.schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: Supply.name, schema: SupplySchema },
      { name: SupplyAllocation.name, schema: SupplyAllocationSchema },
      { name: SupplyHistory.name, schema: SupplyHistorySchema },
      { name: Clinic.name, schema: ClinicSchema },
    ]),
  ],
  controllers: [SupplyController],
  providers: [SupplyService],
  exports: [SupplyService],
})
export class SupplyModule {}
