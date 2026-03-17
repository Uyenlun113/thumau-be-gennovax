import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { WorkSchedule } from './schemas/work-schedule.schema';
import { SetScheduleDto } from './dto/set-schedule.dto';

@Injectable()
export class WorkScheduleService {
  constructor(
    @InjectModel(WorkSchedule.name)
    private workScheduleModel: Model<WorkSchedule>,
  ) {}

  async setSchedule(data: SetScheduleDto): Promise<WorkSchedule> {
    const { userId, date, caLamViec } = data;
    return this.workScheduleModel.findOneAndUpdate(
      { userId, date },
      { userId, date, caLamViec },
      { upsert: true, new: true }
    ).exec();
  }

  async getSchedule(userId: string, date: string): Promise<WorkSchedule | null> {
    return this.workScheduleModel.findOne({ userId, date }).exec();
  }

  async getSchedulesByMonth(year: number, month: number): Promise<WorkSchedule[]> {
    const startDate = `${year}-${String(month).padStart(2, '0')}-01`;
    const endDate = `${year}-${String(month).padStart(2, '0')}-31`;
    
    return this.workScheduleModel.find({
      date: { $gte: startDate, $lte: endDate }
    }).exec();
  }

  async getSchedulesByUser(userId: string, startDate: string, endDate: string): Promise<WorkSchedule[]> {
    return this.workScheduleModel.find({
      userId,
      date: { $gte: startDate, $lte: endDate }
    }).exec();
  }

  async deleteSchedule(userId: string, date: string): Promise<void> {
    await this.workScheduleModel.deleteOne({ userId, date }).exec();
  }
}
