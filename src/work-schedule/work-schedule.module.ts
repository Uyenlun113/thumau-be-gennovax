import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { WorkScheduleController } from './work-schedule.controller';
import { WorkScheduleService } from './work-schedule.service';
import { WorkSchedule, WorkScheduleSchema } from './schemas/work-schedule.schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: WorkSchedule.name, schema: WorkScheduleSchema },
    ]),
  ],
  controllers: [WorkScheduleController],
  providers: [WorkScheduleService],
  exports: [WorkScheduleService],
})
export class WorkScheduleModule {}
