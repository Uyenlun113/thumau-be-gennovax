import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Role, Permission } from './schemas/role.schema';
import { defaultRoles } from './seed-roles';

@Injectable()
export class RoleService {
  constructor(@InjectModel(Role.name) private roleModel: Model<Role>) {}

  async create(data: any): Promise<Role> {
    const role = new this.roleModel(data);
    return role.save();
  }

  async findAll(): Promise<Role[]> {
    return this.roleModel.find().exec();
  }

  async findAllWithPagination(params: {
    search?: string;
    page?: number;
    limit?: number;
  }): Promise<{
    data: Role[];
    total: number;
    page: number;
    limit: number;
    totalPages: number;
  }> {
    const { search, page = 1, limit = 10 } = params;

    const filter: any = {};

    if (search) {
      filter.tenVaiTro = { $regex: search, $options: 'i' };
    }

    const skip = (page - 1) * limit;
    const total = await this.roleModel.countDocuments(filter).exec();

    const data = await this.roleModel
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

  async findOne(id: string): Promise<Role> {
    return this.roleModel.findById(id).exec();
  }

  async update(id: string, data: any): Promise<Role> {
    return this.roleModel.findByIdAndUpdate(id, data, { new: true }).exec();
  }

  async delete(id: string): Promise<Role> {
    return this.roleModel.findByIdAndDelete(id).exec();
  }

  // Seed dữ liệu vai trò mặc định
  async seedDefaultRoles() {
    // Tìm Admin role
    const adminRole = await this.roleModel.findOne({ tenVaiTro: 'Admin' }).exec();
    
    if (adminRole) {
      // Update Admin role với tất cả permissions
      const allPermissions = Object.values(Permission);
      adminRole.permissions = allPermissions;
      await adminRole.save();
      
      return {
        message: 'Admin role updated with all permissions',
        count: allPermissions.length,
        permissions: allPermissions,
      };
    }

    // Nếu chưa có roles, tạo mới
    const existingRoles = await this.roleModel.countDocuments().exec();
    
    if (existingRoles > 0) {
      return {
        message: 'Roles already exist but Admin not found',
        count: existingRoles,
      };
    }

    const roles = await this.roleModel.insertMany(defaultRoles);
    
    return {
      message: 'Default roles seeded successfully',
      count: roles.length,
      roles,
    };
  }

  // Lấy danh sách tất cả permissions
  getAllPermissions() {
    return {
      permissions: Object.values(Permission),
      total: Object.values(Permission).length,
    };
  }
}
