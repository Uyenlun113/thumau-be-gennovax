import { Injectable, UnauthorizedException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { JwtService } from '@nestjs/jwt';
import { Model } from 'mongoose';
import * as bcrypt from 'bcrypt';
import { User } from '../user/schemas/user.schema';
import { LoginHistory } from './schemas/login-history.schema';
import { LoginDto } from './dto/login.dto';
import { LoginDeviceDto } from './dto/login-device.dto';

@Injectable()
export class AuthService {
  constructor(
    @InjectModel(User.name) private userModel: Model<User>,
    @InjectModel(LoginHistory.name) private loginHistoryModel: Model<LoginHistory>,
    private jwtService: JwtService,
  ) {}

  async login(loginDto: LoginDto) {
    const { username, password, ...deviceInfo } = loginDto;

    // Tìm user theo username hoặc maNhanVien
    const user = await this.userModel
      .findOne({
        $or: [{ username }, { maNhanVien: username }],
      })
      .populate('vaiTro')
      .exec();

    if (!user) {
      throw new UnauthorizedException('Tên đăng nhập hoặc mật khẩu không đúng');
    }

    // Kiểm tra password với bcrypt
    const isPasswordValid = await bcrypt.compare(password, user.password);
    if (!isPasswordValid) {
      throw new UnauthorizedException('Tên đăng nhập hoặc mật khẩu không đúng');
    }

    // Kiểm tra tài khoản có đang hoạt động không
    if (!user.dangHoatDong) {
      throw new UnauthorizedException('Tài khoản đã bị vô hiệu hóa');
    }

    // Lưu lịch sử đăng nhập với thông tin thiết bị
    const loginHistory = new this.loginHistoryModel({
      userId: user._id,
      loginTime: new Date(),
      ipAddress: deviceInfo.ipAddress,
      userAgent: deviceInfo.userAgent,
      deviceType: deviceInfo.deviceType,
      browser: deviceInfo.browser,
      os: deviceInfo.os,
      deviceName: deviceInfo.deviceName,
      location: deviceInfo.location,
      isActive: true,
    });
    await loginHistory.save();

    // Tạo JWT token
    const payload = { 
      sub: user._id, 
      username: user.username,
      maNhanVien: user.maNhanVien,
      sessionId: loginHistory._id, // Lưu session ID để logout sau
    };
    const accessToken = this.jwtService.sign(payload);

    // Trả về thông tin user (không bao gồm password) và token
    const userObject = user.toObject();
    delete userObject.password;

    // Lấy danh sách permissions từ role
    const permissions = (user.vaiTro as any)?.permissions || [];

    return {
      success: true,
      message: 'Đăng nhập thành công',
      user: {
        ...userObject,
        permissions, // Thêm permissions vào user object
      },
      accessToken,
      sessionId: loginHistory._id,
    };
  }

  async validateUser(userId: string) {
    const user = await this.userModel
      .findById(userId)
      .populate('vaiTro')
      .exec();
    
    if (!user || !user.dangHoatDong) {
      return null;
    }

    // Lấy permissions trước khi convert to object
    const vaiTro = user.vaiTro as any;
    const permissions = vaiTro?.permissions || [];
    
    // Convert to plain object
    const userObject = user.toObject();
    delete userObject.password;
    
    // Trả về user object với permissions ở top level
    return {
      ...userObject,
      permissions, // Đảm bảo permissions luôn có ở top level
    };
  }

  // Lưu thông tin thiết bị khi đăng nhập
  async saveLoginDevice(userId: string, deviceInfo: LoginDeviceDto) {
    const loginHistory = new this.loginHistoryModel({
      userId,
      loginTime: new Date(),
      ipAddress: deviceInfo.ipAddress,
      userAgent: deviceInfo.userAgent,
      deviceType: deviceInfo.deviceType,
      browser: deviceInfo.browser,
      os: deviceInfo.os,
      deviceName: deviceInfo.deviceName,
      location: deviceInfo.location,
      isActive: true,
    });
    return loginHistory.save();
  }

  // Lấy lịch sử đăng nhập của user
  async getLoginHistory(userId: string, limit: number = 50) {
    return this.loginHistoryModel
      .find({ userId })
      .sort({ loginTime: -1 })
      .limit(limit)
      .populate('userId', 'hoTen maNhanVien')
      .exec();
  }

  // Logout - đánh dấu session không còn active
  async logout(sessionId: string) {
    return this.loginHistoryModel.findByIdAndUpdate(
      sessionId,
      {
        logoutTime: new Date(),
        isActive: false,
      },
      { new: true }
    ).exec();
  }

  // Lấy tất cả session đang active của user
  async getActiveSessions(userId: string) {
    return this.loginHistoryModel
      .find({ userId, isActive: true })
      .sort({ loginTime: -1 })
      .exec();
  }

  // Lấy tất cả lịch sử đăng nhập (tất cả users)
  async getAllLoginHistory(limit: number = 500) {
    return this.loginHistoryModel
      .find()
      .sort({ loginTime: -1 })
      .limit(limit)
      .populate('userId', 'hoTen maNhanVien username')
      .exec();
  }
}
