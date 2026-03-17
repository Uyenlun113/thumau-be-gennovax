import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Clinic } from './schemas/clinic.schema';

@Injectable()
export class ClinicService {
  constructor(@InjectModel(Clinic.name) private clinicModel: Model<Clinic>) {}

  async create(data: any): Promise<Clinic> {
    // Auto-generate maPhongKham if not provided
    if (!data.maPhongKham) {
      // Find the highest existing clinic code
      const lastClinic = await this.clinicModel
        .findOne({ maPhongKham: /^PK\d+$/ })
        .sort({ maPhongKham: -1 })
        .exec();

      let nextNumber = 1;
      if (lastClinic && lastClinic.maPhongKham) {
        const lastNumber = parseInt(lastClinic.maPhongKham.replace('PK', ''));
        nextNumber = lastNumber + 1;
      }

      data.maPhongKham = `PK${String(nextNumber).padStart(4, '0')}`;
    }
    
    const clinic = new this.clinicModel(data);
    return clinic.save();
  }

  async findAll(): Promise<Clinic[]> {
    return this.clinicModel.find().exec();
  }

  async findAllWithPagination(params: {
    status?: boolean;
    search?: string;
    page?: number;
    limit?: number;
  }): Promise<{
    data: Clinic[];
    total: number;
    page: number;
    limit: number;
    totalPages: number;
  }> {
    const { status, search, page = 1, limit = 10 } = params;

    const filter: any = {};

    if (status !== undefined) {
      filter.dangHoatDong = status;
    }

    if (search) {
      filter.$or = [
        { tenPhongKham: { $regex: search, $options: 'i' } },
        { maPhongKham: { $regex: search, $options: 'i' } },
        { diaChi: { $regex: search, $options: 'i' } },
        { chuyenKhoa: { $regex: search, $options: 'i' } },
      ];
    }

    const skip = (page - 1) * limit;
    const total = await this.clinicModel.countDocuments(filter).exec();

    const data = await this.clinicModel
      .find(filter)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit)
      .exec();

    return {
      data,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findOne(id: string): Promise<Clinic> {
    return this.clinicModel.findById(id).exec();
  }

  async findByCode(maPhongKham: string): Promise<Clinic> {
    return this.clinicModel.findOne({ maPhongKham }).exec();
  }

  async update(id: string, data: any): Promise<Clinic> {
    return this.clinicModel.findByIdAndUpdate(id, data, { new: true }).exec();
  }

  async delete(id: string): Promise<Clinic> {
    return this.clinicModel.findByIdAndDelete(id).exec();
  }

  async findNearby(location: { lat: number; lng: number }, maxDistance: number = 10): Promise<any[]> {
    // Tìm phòng khám gần vị trí cho trước
    const clinics = await this.clinicModel.find({
      dangHoatDong: true,
      toaDo: { $exists: true }
    }).exec();

    // Tính khoảng cách và lọc
    const clinicsWithDistance = clinics
      .map(clinic => {
        if (clinic.toaDo) {
          const distance = this.calculateDistance(
            location.lat,
            location.lng,
            clinic.toaDo.lat,
            clinic.toaDo.lng
          );
          return { ...clinic.toObject(), distance };
        }
        return null;
      })
      .filter(clinic => clinic !== null && clinic.distance <= maxDistance)
      .sort((a, b) => a.distance - b.distance);

    return clinicsWithDistance;
  }

  // Hàm tính khoảng cách Haversine
  private calculateDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const R = 6371; // Bán kính Trái Đất (km)
    const dLat = this.deg2rad(lat2 - lat1);
    const dLon = this.deg2rad(lon2 - lon1);
    const a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(this.deg2rad(lat1)) *
        Math.cos(this.deg2rad(lat2)) *
        Math.sin(dLon / 2) *
        Math.sin(dLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    const distance = R * c;
    return distance;
  }

  private deg2rad(deg: number): number {
    return deg * (Math.PI / 180);
  }
}
