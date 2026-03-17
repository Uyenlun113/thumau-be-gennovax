import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { CreateBusStationDto } from './dto/create-bus-station.dto';
import { UpdateBusStationDto } from './dto/update-bus-station.dto';
import { BusStation } from './schemas/bus-station.schema';

@Injectable()
export class BusStationService {
  constructor(
    @InjectModel(BusStation.name) private busStationModel: Model<BusStation>,
  ) {}

  async create(createBusStationDto: CreateBusStationDto): Promise<BusStation> {
    // Auto-generate maNhaXe if not provided
    if (!createBusStationDto.maNhaXe) {
      // Find the highest existing bus station code
      const lastBusStation = await this.busStationModel
        .findOne({ maNhaXe: /^NX\d+$/ })
        .sort({ maNhaXe: -1 })
        .exec();

      let nextNumber = 1;
      if (lastBusStation && lastBusStation.maNhaXe) {
        const lastNumber = parseInt(lastBusStation.maNhaXe.replace('NX', ''));
        nextNumber = lastNumber + 1;
      }

      createBusStationDto.maNhaXe = `NX${String(nextNumber).padStart(4, '0')}`;
    }
    
    const busStation = new this.busStationModel(createBusStationDto);
    return busStation.save();
  }

  async findAll(params?: any): Promise<{ data: BusStation[]; total: number }> {
    const { page = 1, limit = 10, search } = params || {};
    const skip = (page - 1) * limit;

    const query: any = {};

    if (search) {
      query.$or = [
        { tenNhaXe: { $regex: search, $options: 'i' } },
        { maNhaXe: { $regex: search, $options: 'i' } },
        { diaChi: { $regex: search, $options: 'i' } },
      ];
    }

    const [data, total] = await Promise.all([
      this.busStationModel.find(query).skip(skip).limit(Number(limit)).sort({ createdAt: -1 }).exec(),
      this.busStationModel.countDocuments(query).exec(),
    ]);

    return { data, total };
  }

  async findOne(id: string): Promise<BusStation> {
    const busStation = await this.busStationModel.findById(id).exec();
    if (!busStation) {
      throw new NotFoundException(`Nhà xe với ID ${id} không tồn tại`);
    }
    return busStation;
  }

  async update(id: string, updateBusStationDto: UpdateBusStationDto): Promise<BusStation> {
    const busStation = await this.busStationModel
      .findByIdAndUpdate(id, updateBusStationDto, { new: true })
      .exec();
    if (!busStation) {
      throw new NotFoundException(`Nhà xe với ID ${id} không tồn tại`);
    }
    return busStation;
  }

  async remove(id: string): Promise<void> {
    const result = await this.busStationModel.findByIdAndDelete(id).exec();
    if (!result) {
      throw new NotFoundException(`Nhà xe với ID ${id} không tồn tại`);
    }
  }
}
