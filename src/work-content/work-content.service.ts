import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { WorkContent } from './schemas/work-content.schema';

@Injectable()
export class WorkContentService {
  constructor(@InjectModel(WorkContent.name) private workContentModel: Model<WorkContent>) {}

  async create(data: any): Promise<WorkContent> {
    const workContent = new this.workContentModel(data);
    return workContent.save();
  }

  async findAll(): Promise<WorkContent[]> {
    return this.workContentModel.find().exec();
  }

  async findOne(id: string): Promise<WorkContent> {
    return this.workContentModel.findById(id).exec();
  }

  async update(id: string, data: any): Promise<WorkContent> {
    return this.workContentModel.findByIdAndUpdate(id, data, { new: true }).exec();
  }

  async delete(id: string): Promise<WorkContent> {
    return this.workContentModel.findByIdAndDelete(id).exec();
  }
}
