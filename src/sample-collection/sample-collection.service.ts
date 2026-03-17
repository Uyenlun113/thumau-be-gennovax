import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Cron, CronExpression } from '@nestjs/schedule';
import { Model, Types } from 'mongoose';
import { EmailService } from '../email/email.service';
import { NotificationService } from '../notification/notification.service';
import { NotificationType } from '../notification/schemas/notification.schema';
import { User } from '../user/schemas/user.schema';
import { SampleCollectionHistory } from './schemas/sample-collection-history.schema';
import { SampleCollection, SampleCollectionStatus } from './schemas/sample-collection.schema';
import { SampleCollectionMessage } from './schemas/sample-collection-message.schema';

@Injectable()
export class SampleCollectionService {
  constructor(
    @InjectModel(SampleCollection.name)
    private sampleCollectionModel: Model<SampleCollection>,
    @InjectModel(SampleCollectionHistory.name)
    private sampleCollectionHistoryModel: Model<SampleCollectionHistory>,
    @InjectModel(SampleCollectionMessage.name)
    private sampleCollectionMessageModel: Model<SampleCollectionMessage>,
    @InjectModel(User.name)
    private userModel: Model<User>,
    private emailService: EmailService,
    private notificationService: NotificationService,
  ) { }

  async create(data: any): Promise<SampleCollection> {
    // Tự động generate mã lệnh theo format: TM-ddmmyy-xxx
    const now = new Date();
    const day = String(now.getDate()).padStart(2, '0');
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const year = String(now.getFullYear()).slice(-2);
    const datePrefix = `TM-${day}${month}${year}`;

    // Tìm số thứ tự cao nhất trong ngày
    const lastOrder = await this.sampleCollectionModel
      .findOne({ maLenh: new RegExp(`^${datePrefix}`) })
      .sort({ maLenh: -1 })
      .exec();

    let sequence = 1;
    if (lastOrder && lastOrder.maLenh) {
      const lastSequence = parseInt(lastOrder.maLenh.split('-')[2]);
      sequence = lastSequence + 1;
    }

    const maLenh = `${datePrefix}-${String(sequence).padStart(3, '0')}`;

    // Nếu không có nguoiGiaoLenh, báo lỗi rõ ràng
    if (!data.nguoiGiaoLenh) {
      throw new Error('nguoiGiaoLenh is required. Please ensure user is logged in.');
    }

    // Ensure uuTien is boolean
    const uuTien = data.uuTien === true || data.uuTien === 'true';

    // Xử lý phongKhamItems
    // Nếu có phongKham (lệnh standard), tạo phongKhamItems với 1 item
    let phongKhamItems = data.phongKhamItems || [];
    if (data.phongKham && phongKhamItems.length === 0) {
      phongKhamItems = [{
        phongKham: data.phongKham,
        soTienCuocNhanMau: 0,
        soTienShip: 0,
        soTienGuiXe: 0,
        anhHoanThanhKiemTra: []
      }];
    }

    const sampleCollection = new this.sampleCollectionModel({
      ...data,
      maLenh,
      uuTien,
      phongKhamItems,
    });

    const saved = await sampleCollection.save();

    // Nếu có giao nhân viên ngay khi tạo (auto-create), tính khoảng cách
    if (data.nhanVienThucHien && phongKhamItems.length > 0) {
      try {
        const employee = await this.userModel.findById(data.nhanVienThucHien).exec();
        const clinicId = phongKhamItems[0].phongKham;
        // Convert to ObjectId if string
        const clinicObjectId = typeof clinicId === 'string' ? new Types.ObjectId(clinicId) : clinicId;
        const clinic = await this.sampleCollectionModel.db.collection('clinics').findOne({ _id: clinicObjectId });
        
        if (employee?.viTriHienTai && clinic?.toaDo?.lat && clinic?.toaDo?.lng) {
          const khoangCach = this.calculateDistance(
            employee.viTriHienTai.lat,
            employee.viTriHienTai.lng,
            clinic.toaDo.lat,
            clinic.toaDo.lng
          );
          await this.sampleCollectionModel.findByIdAndUpdate(saved._id, { khoangCachDiChuyen: khoangCach }).exec();
        }
      } catch (error) {
        console.error('Error calculating distance in auto-create:', error);
      }
    }

    // Lưu lịch sử: Tạo lệnh (không có trạng thái trước đó)
    await this.saveHistory(
      saved._id.toString(),
      null, // Không có trạng thái trước
      SampleCollectionStatus.CHO_DIEU_PHOI,
      data.nguoiGiaoLenh,
      'Tạo lệnh thu mẫu',
    );

    // Tạo thông báo CHỈ cho Admin khi tạo lệnh mới
    const admins = await this.getAdminUsers();

    // Lấy thông tin phòng khám hoặc nhà xe
    let locationInfo = '';
    if (saved.tenNhaXe) {
      locationInfo = `nhà xe ${saved.tenNhaXe}`;
    } else if (phongKhamItems.length > 0) {
      // Populate để lấy tên phòng khám
      const populatedOrder = await this.sampleCollectionModel
        .findById(saved._id)
        .populate('phongKhamItems.phongKham')
        .exec();

      if (populatedOrder && populatedOrder.phongKhamItems && populatedOrder.phongKhamItems.length > 0) {
        const firstClinic = populatedOrder.phongKhamItems[0].phongKham as any;
        locationInfo = `phòng khám ${firstClinic?.tenPhongKham || 'N/A'}`;
      }
    }

    for (const admin of admins) {
      await this.notificationService.create({
        userId: admin._id.toString(),
        title: 'Lệnh thu mẫu mới',
        message: `Lệnh ${maLenh} đã được tạo${locationInfo ? ` tại ${locationInfo}` : ''}${saved.uuTien ? ' (GẤP)' : ''}`,
        type: NotificationType.ORDER_ASSIGNED,
        relatedOrderId: saved._id.toString(),
      });
    }

    return saved;
  }

  // Helper method để lấy danh sách Admin
  private async getAdminUsers(): Promise<User[]> {
    const users = await this.userModel.find().populate('vaiTro').exec();
    return users.filter(user => {
      if (user.vaiTro && typeof user.vaiTro === 'object') {
        return (user.vaiTro as any).tenVaiTro === 'Admin';
      }
      return false;
    });
  }

  async findAll(filter: any = {}): Promise<SampleCollection[]> {
    return this.sampleCollectionModel
      .find(filter)
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .exec();
  }

  async findAllWithPagination(params: {
    status?: string;
    search?: string;
    employeeId?: string;
    clinicId?: string;
    page?: number;
    limit?: number;
    currentUser?: any;
    startDate?: string;
    endDate?: string;
  }): Promise<{
    data: SampleCollection[];
    total: number;
    page: number;
    limit: number;
    totalPages: number;
  }> {
    const { status, search, employeeId, clinicId, page = 1, limit = 10, currentUser, startDate, endDate } = params;

    // Build query filter
    const filter = await this.buildQueryFilter({ status, search, employeeId, clinicId, currentUser, startDate, endDate });

    // Calculate skip
    const skip = (page - 1) * limit;

    // Get total count
    const total = await this.sampleCollectionModel.countDocuments(filter).exec();

    // Get paginated data
    const data = await this.sampleCollectionModel
      .find(filter)
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .sort({ createdAt: -1 }) // Newest first
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

  // Helper method để build query filter (dùng chung cho list và count)
  private async buildQueryFilter(params: {
    status?: string;
    search?: string;
    employeeId?: string;
    clinicId?: string;
    currentUser?: any;
    startDate?: string;
    endDate?: string;
  }): Promise<any> {
    const { status, search, employeeId, clinicId, currentUser, startDate, endDate } = params;

    const filter: any = {};
    const andConditions: any[] = [];

    // Filter by status
    if (status) {
      filter.trangThai = status;
    }

    // Filter by date range
    if (startDate || endDate) {
      filter.createdAt = {};
      if (startDate) {
        filter.createdAt.$gte = new Date(startDate);
      }
      if (endDate) {
        const end = new Date(endDate);
        end.setHours(23, 59, 59, 999);
        filter.createdAt.$lte = end;
      }
    }

    // Filter by clinic (search in phongKhamItems array OR old phongKham field)
    if (clinicId) {
      andConditions.push({
        $or: [
          { 'phongKhamItems.phongKham': clinicId },
          { 'phongKham': clinicId }
        ]
      });
    }

    // Filter by employee - LOGIC MỚI DỰA TRÊN PERMISSION
    // Kiểm tra quyền xem lệnh
    const hasViewAllPermission = currentUser?.permissions?.includes('ORDER_VIEW_ALL');

    if (employeeId && !hasViewAllPermission) {
      // Nếu có quyền ORDER_VIEW_OWN hoặc không có quyền ORDER_VIEW_ALL
      // Áp dụng logic filter theo status
      const isCompletedStatus = status === SampleCollectionStatus.HOAN_THANH ||
        status === SampleCollectionStatus.HOAN_THANH_KIEM_TRA;

      if (!isCompletedStatus) {
        // Với status khác HOAN_THANH và HOAN_THANH_KIEM_TRA: chỉ xem lệnh được giao cho mình
        andConditions.push({
          $or: [
            { nguoiGiaoLenh: employeeId },
            { nhanVienThucHien: employeeId }
          ]
        });
      }
      // Với status HOAN_THANH hoặc HOAN_THANH_KIEM_TRA: không filter (xem tất cả)
    }
    // Nếu có quyền ORDER_VIEW_ALL: không filter theo employeeId (xem tất cả)

    // Search by maLenh or noiDungCongViec
    if (search) {
      const searchConditions: any[] = [
        { maLenh: { $regex: search, $options: 'i' } }, // Case-insensitive search
      ];

      // Nếu search có thể là ObjectId, tìm theo noiDungCongViec
      // Nhưng vì noiDungCongViec là reference, ta cần populate trước
      // Để đơn giản, ta sẽ tìm work content trước
      const workContents = await this.sampleCollectionModel.db.collection('workcontents').find({
        tenCongViec: { $regex: search, $options: 'i' }
      }).toArray();

      if (workContents.length > 0) {
        searchConditions.push({
          noiDungCongViec: { $in: workContents.map(wc => wc._id) }
        });
      }

      andConditions.push({ $or: searchConditions });
    }

    // Combine all conditions with $and if there are multiple
    if (andConditions.length > 0) {
      filter.$and = andConditions;
    }

    return filter;
  }


  // API count theo từng status (gọi 1 lần trả về tất cả)
  async countByStatus(params: {
    search?: string;
    employeeId?: string;
    clinicId?: string;
    currentUser?: any;
    startDate?: string;
    endDate?: string;
  }): Promise<{
    all: number;
    CHO_DIEU_PHOI: number;
    DANG_THUC_HIEN: number;
    HOAN_THANH: number;
    HOAN_THANH_KIEM_TRA: number;
    DA_HUY: number;
  }> {
    // Get base filter without status
    const baseFilter = await this.buildQueryFilter(params);

    // Count all
    const all = await this.sampleCollectionModel.countDocuments(baseFilter).exec();

    // Count by each status
    const [CHO_DIEU_PHOI, DANG_THUC_HIEN, HOAN_THANH, HOAN_THANH_KIEM_TRA, DA_HUY] = await Promise.all([
      this.sampleCollectionModel.countDocuments({ ...baseFilter, trangThai: SampleCollectionStatus.CHO_DIEU_PHOI }).exec(),
      this.sampleCollectionModel.countDocuments({ ...baseFilter, trangThai: SampleCollectionStatus.DANG_THUC_HIEN }).exec(),
      this.sampleCollectionModel.countDocuments({ ...baseFilter, trangThai: SampleCollectionStatus.HOAN_THANH }).exec(),
      this.sampleCollectionModel.countDocuments({ ...baseFilter, trangThai: SampleCollectionStatus.HOAN_THANH_KIEM_TRA }).exec(),
      this.sampleCollectionModel.countDocuments({ ...baseFilter, trangThai: SampleCollectionStatus.DA_HUY }).exec(),
    ]);

    return {
      all,
      CHO_DIEU_PHOI,
      DANG_THUC_HIEN,
      HOAN_THANH,
      HOAN_THANH_KIEM_TRA,
      DA_HUY,
    };
  }

  async findOne(id: string): Promise<SampleCollection> {
    return this.sampleCollectionModel
      .findById(id)
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .exec();
  }

  async findByCode(maLenh: string): Promise<SampleCollection> {
    return this.sampleCollectionModel
      .findOne({ maLenh })
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .exec();
  }

  async update(id: string, data: any): Promise<SampleCollection> {
    const oldData = await this.sampleCollectionModel.findById(id).exec();

    // Ensure uuTien is boolean if provided
    if (data.uuTien !== undefined) {
      data.uuTien = data.uuTien === true || data.uuTien === 'true';
    }

    // Handle phongKham update - update phongKhamItems[0] instead of old phongKham field
    if (data.phongKham) {
      // Get existing phongKhamItems or create new array
      // Convert to plain objects to avoid Mongoose metadata issues
      const existingItems = oldData.phongKhamItems && oldData.phongKhamItems.length > 0 
        ? oldData.phongKhamItems.map(item => ({
            phongKham: item.phongKham,
            soTienCuocNhanMau: item.soTienCuocNhanMau,
            soTienShip: item.soTienShip,
            soTienGuiXe: item.soTienGuiXe,
            anhHoanThanhKiemTra: item.anhHoanThanhKiemTra || [],
            thoiGianHoanThanhKiemTra: item.thoiGianHoanThanhKiemTra,
          }))
        : [];

      if (existingItems.length > 0) {
        // Update existing first item
        existingItems[0] = {
          ...existingItems[0],
          phongKham: data.phongKham,
          // Update cost fields if provided
          soTienCuocNhanMau: data.soTienCuocNhanMau !== undefined ? data.soTienCuocNhanMau : existingItems[0].soTienCuocNhanMau,
          soTienShip: data.soTienShip !== undefined ? data.soTienShip : existingItems[0].soTienShip,
          soTienGuiXe: data.soTienGuiXe !== undefined ? data.soTienGuiXe : existingItems[0].soTienGuiXe,
        };
      } else {
        // Create new phongKhamItems array with one item
        existingItems.push({
          phongKham: data.phongKham,
          soTienCuocNhanMau: data.soTienCuocNhanMau || 0,
          soTienShip: data.soTienShip || 0,
          soTienGuiXe: data.soTienGuiXe || 0,
          anhHoanThanhKiemTra: [],
          thoiGianHoanThanhKiemTra: undefined,
        });
      }
      
      // Replace phongKham with phongKhamItems in data
      data.phongKhamItems = existingItems;
      
      delete data.phongKham;
      delete data.soTienCuocNhanMau;
      delete data.soTienShip;
      delete data.soTienGuiXe;
    }

    // Handle phongKhamItems update (for bus station orders)
    if (data.phongKhamItems && Array.isArray(data.phongKhamItems)) {
      // Convert to plain objects to avoid Mongoose metadata issues
      data.phongKhamItems = data.phongKhamItems.map(item => ({
        phongKham: item.phongKham,
        soTienCuocNhanMau: item.soTienCuocNhanMau || 0,
        soTienShip: item.soTienShip || 0,
        soTienGuiXe: item.soTienGuiXe || 0,
        anhHoanThanhKiemTra: item.anhHoanThanhKiemTra || [],
        thoiGianHoanThanhKiemTra: item.thoiGianHoanThanhKiemTra,
      }));
    }

    const result = await this.sampleCollectionModel
      .findByIdAndUpdate(id, data, { new: true })
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .exec();

    // Nếu có thay đổi trạng thái, lưu lịch sử
    if (result && data.trangThai && oldData.trangThai !== data.trangThai) {
      let ghiChu = '';
      // Ưu tiên lấy nguoiThucHien từ data (người thực hiện thay đổi)
      // Nếu không có thì lấy nhanVienThucHien (người được giao)
      const nguoiThucHien = data.nguoiThucHien || result.nhanVienThucHien?._id?.toString();

      switch (data.trangThai) {
        case SampleCollectionStatus.CHO_DIEU_PHOI:
          ghiChu = 'Chuyển về chờ điều phối';
          break;
        case SampleCollectionStatus.DANG_THUC_HIEN:
          ghiChu = 'Bắt đầu thực hiện lệnh';
          break;
        case SampleCollectionStatus.HOAN_THANH:
          ghiChu = 'Hoàn thành thu mẫu';
          break;
        case SampleCollectionStatus.HOAN_THANH_KIEM_TRA:
          ghiChu = 'Hoàn thành kiểm tra';
          break;
        case SampleCollectionStatus.DA_HUY:
          ghiChu = 'Hủy lệnh';
          break;
        default:
          ghiChu = 'Cập nhật trạng thái';
      }

      await this.saveHistory(
        id,
        oldData.trangThai, // Trạng thái trước đó
        data.trangThai as SampleCollectionStatus, // Trạng thái mới
        nguoiThucHien,
        ghiChu,
      );

      // Gửi thông báo khi thay đổi trạng thái
      await this.sendStatusChangeNotifications(result, ghiChu);
    }

    // Nếu có giao nhân viên mới (điều phối lệnh)
    if (result && data.nhanVienThucHien && oldData.nhanVienThucHien?.toString() !== data.nhanVienThucHien) {
      // Lấy thông tin nhân viên được giao
      const assignedEmployee = await this.userModel.findById(data.nhanVienThucHien).exec();
      const employeeName = assignedEmployee?.hoTen || 'nhân viên';

      // Tính khoảng cách từ nhân viên đến phòng khám
      let khoangCach = 0;
      if (assignedEmployee?.viTriHienTai && result.phongKhamItems && result.phongKhamItems.length > 0) {
        const firstClinic = result.phongKhamItems[0].phongKham as any;
        if (firstClinic?.toaDo?.lat && firstClinic?.toaDo?.lng) {
          khoangCach = this.calculateDistance(
            assignedEmployee.viTriHienTai.lat,
            assignedEmployee.viTriHienTai.lng,
            firstClinic.toaDo.lat,
            firstClinic.toaDo.lng
          );
          // Lưu khoảng cách vào lệnh
          await this.sampleCollectionModel.findByIdAndUpdate(id, { khoangCachDiChuyen: khoangCach }).exec();
        }
      }

      // Lấy thông tin phòng khám hoặc nhà xe
      let locationInfo = '';
      if (result.tenNhaXe) {
        locationInfo = ` tại nhà xe ${result.tenNhaXe}`;
      } else if (result.phongKhamItems && result.phongKhamItems.length > 0) {
        const firstClinic = result.phongKhamItems[0].phongKham as any;
        locationInfo = ` tại phòng khám ${firstClinic?.tenPhongKham || 'N/A'}`;
      }

      // Gửi thông báo cho: người được giao, người tạo lệnh, và Admin
      const recipientIds = new Set<string>();

      // 1. Người được giao
      recipientIds.add(data.nhanVienThucHien);

      // 2. Người tạo lệnh
      if (result.nguoiGiaoLenh) {
        const nguoiGiaoLenhId = typeof result.nguoiGiaoLenh === 'object'
          ? (result.nguoiGiaoLenh as any)._id.toString()
          : (result.nguoiGiaoLenh as any).toString();
        recipientIds.add(nguoiGiaoLenhId);
      }

      // 3. Tất cả Admin
      const admins = await this.getAdminUsers();
      admins.forEach(admin => recipientIds.add(admin._id.toString()));

      // Gửi thông báo cho tất cả người nhận với thông tin chi tiết
      for (const userId of recipientIds) {
        // Tùy chỉnh message cho từng người nhận
        let message = '';
        if (userId === data.nhanVienThucHien) {
          // Người được giao
          message = `Bạn được giao lệnh ${result.maLenh}${locationInfo}${result.uuTien ? ' (GẤP)' : ''}`;
        } else {
          // Người tạo lệnh và Admin
          message = `Lệnh ${result.maLenh} đã được giao cho ${employeeName}${locationInfo}${result.uuTien ? ' (GẤP)' : ''}`;
        }

        await this.notificationService.create({
          userId,
          title: 'Điều phối lệnh thu mẫu',
          message,
          type: NotificationType.ORDER_ASSIGNED,
          relatedOrderId: id,
        });
      }
    }

    return result;
  }

  // Helper method để gửi thông báo khi thay đổi trạng thái
  private async sendStatusChangeNotifications(order: SampleCollection, ghiChu: string) {
    const recipientIds = new Set<string>();

    // 1. Người tạo lệnh
    if (order.nguoiGiaoLenh) {
      const nguoiGiaoLenhId = typeof order.nguoiGiaoLenh === 'object'
        ? (order.nguoiGiaoLenh as any)._id.toString()
        : (order.nguoiGiaoLenh as any).toString();
      recipientIds.add(nguoiGiaoLenhId);
    }

    // 2. Người được giao
    if (order.nhanVienThucHien) {
      const nhanVienThucHienId = typeof order.nhanVienThucHien === 'object'
        ? (order.nhanVienThucHien as any)._id.toString()
        : (order.nhanVienThucHien as any).toString();
      recipientIds.add(nhanVienThucHienId);
    }

    // 3. Tất cả Admin
    const admins = await this.getAdminUsers();
    admins.forEach(admin => recipientIds.add(admin._id.toString()));

    // Lấy thông tin phòng khám hoặc nhà xe
    let locationInfo = '';
    if (order.tenNhaXe) {
      locationInfo = ` tại nhà xe ${order.tenNhaXe}`;
    } else if (order.phongKhamItems && order.phongKhamItems.length > 0) {
      const firstClinic = order.phongKhamItems[0].phongKham as any;
      locationInfo = ` tại phòng khám ${firstClinic?.tenPhongKham || 'N/A'}`;
    }

    // Lấy tên nhân viên thực hiện
    const employeeName = order.nhanVienThucHien
      ? (typeof order.nhanVienThucHien === 'object'
        ? (order.nhanVienThucHien as any).hoTen
        : 'nhân viên')
      : 'nhân viên';

    // Gửi thông báo cho tất cả người nhận
    for (const userId of recipientIds) {
      let message = '';

      // Tùy chỉnh message dựa trên trạng thái
      switch (order.trangThai) {
        case SampleCollectionStatus.DANG_THUC_HIEN:
          message = `Lệnh ${order.maLenh}${locationInfo} đang được thực hiện bởi ${employeeName}`;
          break;
        case SampleCollectionStatus.HOAN_THANH:
          message = `Lệnh ${order.maLenh}${locationInfo} đã hoàn thành bởi ${employeeName}`;
          break;
        case SampleCollectionStatus.HOAN_THANH_KIEM_TRA:
          message = `Lệnh ${order.maLenh}${locationInfo} đã hoàn thành kiểm tra`;
          break;
        case SampleCollectionStatus.DA_HUY:
          message = `Lệnh ${order.maLenh}${locationInfo} đã bị hủy`;
          break;
        default:
          message = `Lệnh ${order.maLenh}${locationInfo}: ${ghiChu}`;
      }

      await this.notificationService.create({
        userId,
        title: 'Cập nhật trạng thái lệnh',
        message,
        type: NotificationType.ORDER_STATUS_CHANGED,
        relatedOrderId: order._id.toString(),
      });
    }
  }

  async assignStaff(id: string, nhanVienThucHien: string): Promise<SampleCollection> {
    const oldData = await this.sampleCollectionModel.findById(id).exec();

    const result = await this.sampleCollectionModel
      .findByIdAndUpdate(
        id,
        {
          nhanVienThucHien,
          trangThai: SampleCollectionStatus.DANG_THUC_HIEN,
        },
        { new: true }
      )
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .exec();

    // Lưu lịch sử: Điều phối lệnh
    if (result) {
      await this.saveHistory(
        id,
        oldData.trangThai, // Trạng thái trước đó
        SampleCollectionStatus.DANG_THUC_HIEN, // Trạng thái mới
        nhanVienThucHien,
        'Điều phối lệnh cho nhân viên',
      );
    }

    return result;
  }

  async updateStatus(
    id: string,
    trangThai: string,
    additionalData?: any
  ): Promise<SampleCollection> {
    const oldData = await this.sampleCollectionModel.findById(id).exec();
    const updateData: any = { trangThai, ...additionalData };

    // Cập nhật thời gian hoàn thành
    if (trangThai === SampleCollectionStatus.HOAN_THANH) {
      updateData.thoiGianHoanThanh = new Date();
    }

    const result = await this.sampleCollectionModel
      .findByIdAndUpdate(id, updateData, { new: true })
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .exec();

    // Lưu lịch sử khi thay đổi trạng thái
    if (result) {
      let ghiChu = '';
      // Ưu tiên lấy nguoiThucHien từ additionalData (người thực hiện thay đổi)
      const nguoiThucHien = additionalData?.nguoiThucHien || result.nhanVienThucHien?._id?.toString();

      switch (trangThai) {
        case SampleCollectionStatus.DANG_THUC_HIEN:
          ghiChu = 'Bắt đầu thực hiện lệnh';
          break;
        case SampleCollectionStatus.HOAN_THANH:
          ghiChu = 'Hoàn thành thu mẫu';
          break;
        case SampleCollectionStatus.HOAN_THANH_KIEM_TRA:
          ghiChu = 'Hoàn thành kiểm tra';
          // Email sẽ chỉ được gửi khi người dùng click nút "Gửi lại email"
          // Không tự động gửi email nữa
          break;
        case SampleCollectionStatus.DA_HUY:
          ghiChu = 'Hủy lệnh';
          break;
        default:
          ghiChu = 'Cập nhật trạng thái';
      }

      await this.saveHistory(
        id,
        oldData.trangThai, // Trạng thái trước đó
        trangThai as SampleCollectionStatus, // Trạng thái mới
        nguoiThucHien,
        ghiChu,
        additionalData,
      );

      // Gửi thông báo cho tất cả người liên quan
      await this.sendStatusChangeNotifications(result, ghiChu);
    }

    return result;
  }

  async delete(id: string): Promise<SampleCollection> {
    return this.sampleCollectionModel.findByIdAndDelete(id).exec();
  }

  async getStatsSummary(): Promise<any> {
    const collections = await this.sampleCollectionModel.find().exec();

    // Tính tổng tiền từ phongKhamItems
    let tongSoTienCuocNhanMau = 0;
    let tongSoTienShip = 0;
    let tongSoTienGuiXe = 0;

    collections.forEach(c => {
      if (c.phongKhamItems && c.phongKhamItems.length > 0) {
        c.phongKhamItems.forEach(item => {
          tongSoTienCuocNhanMau += item.soTienCuocNhanMau || 0;
          tongSoTienShip += item.soTienShip || 0;
          tongSoTienGuiXe += item.soTienGuiXe || 0;
        });
      }
    });

    const stats = {
      total: collections.length,
      choDieuPhoi: collections.filter(c => c.trangThai === SampleCollectionStatus.CHO_DIEU_PHOI).length,
      dangThucHien: collections.filter(c => c.trangThai === SampleCollectionStatus.DANG_THUC_HIEN).length,
      hoanThanh: collections.filter(c => c.trangThai === SampleCollectionStatus.HOAN_THANH).length,
      hoanThanhKiemTra: collections.filter(c => c.trangThai === SampleCollectionStatus.HOAN_THANH_KIEM_TRA).length,
      daHuy: collections.filter(c => c.trangThai === SampleCollectionStatus.DA_HUY).length,
      tongSoTienCuocNhanMau,
      tongSoTienShip,
      tongSoTienGuiXe,
    };

    return stats;
  }

  async findByStaff(staffId: string): Promise<SampleCollection[]> {
    return this.sampleCollectionModel
      .find({ nhanVienThucHien: staffId })
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .exec();
  }

  async findByClinic(clinicId: string): Promise<SampleCollection[]> {
    return this.sampleCollectionModel
      .find({ 'phongKhamItems.phongKham': clinicId })
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .exec();
  }

  async exportToExcel(filters?: {
    status?: string;
    search?: string;
    employeeId?: string;
    clinicId?: string;
    startDate?: string;
    endDate?: string;
    currentUser?: any;
  }): Promise<any> {
    const ExcelJS = require('exceljs');
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Lệnh Thu Mẫu');

    // Build filter query
    const filter = filters ? await this.buildQueryFilter(filters) : {};

    // Fetch data with filters
    const collections = await this.sampleCollectionModel
      .find(filter)
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .sort({ createdAt: -1 })
      .exec();

    // Define columns
    worksheet.columns = [
      { header: 'Mã lệnh', key: 'maLenh', width: 15 },
      { header: 'Nhà xe', key: 'nhaXe', width: 25 },
      { header: 'Phòng khám', key: 'phongKham', width: 30 },
      { header: 'Nội dung công việc', key: 'noiDungCongViec', width: 25 },
      { header: 'Trạng thái', key: 'trangThai', width: 20 },
      { header: 'Ưu tiên', key: 'uuTien', width: 10 },
      { header: 'Người giao lệnh', key: 'nguoiGiaoLenh', width: 20 },
      { header: 'Nhân viên thực hiện', key: 'nhanVienThucHien', width: 20 },
      { header: 'Khoảng cách (km)', key: 'khoangCach', width: 15 },
      { header: 'Thời gian tạo', key: 'createdAt', width: 20 },
      { header: 'Hạn hoàn thành', key: 'thoiGianHenHoanThanh', width: 20 },
      { header: 'Ghi chú', key: 'ghiChu', width: 30 },
      { header: 'Cước nhận mẫu', key: 'soTienCuocNhanMau', width: 15 },
      { header: 'Phí ship', key: 'soTienShip', width: 15 },
      { header: 'Phí gửi xe', key: 'soTienGuiXe', width: 15 },
      { header: 'Tổng tiền', key: 'tongTien', width: 15 },
    ];

    // Style header row
    worksheet.getRow(1).font = { bold: true, size: 12 };
    worksheet.getRow(1).fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: 'FF4472C4' },
    };
    worksheet.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
    worksheet.getRow(1).alignment = { vertical: 'middle', horizontal: 'center' };

    // Status labels mapping
    const statusLabels: Record<string, string> = {
      CHO_DIEU_PHOI: 'Chờ điều phối',
      DANG_THUC_HIEN: 'Đang thực hiện',
      HOAN_THANH: 'Hoàn thành',
      HOAN_THANH_KIEM_TRA: 'Hoàn thành kiểm tra',
      DA_HUY: 'Đã hủy',
    };

    // Add data rows
    collections.forEach((collection: any) => {
      // For bus station orders with multiple clinics, create one row per clinic
      if (collection.tenNhaXe && collection.phongKhamItems && collection.phongKhamItems.length > 0) {
        const startRow = worksheet.rowCount + 1;
        
        collection.phongKhamItems.forEach((item: any, index: number) => {
          const row = worksheet.addRow({
            maLenh: index === 0 ? collection.maLenh : '',
            nhaXe: index === 0 ? collection.tenNhaXe : '',
            phongKham: item.phongKham?.tenPhongKham || 'N/A',
            noiDungCongViec: index === 0 ? collection.noiDungCongViec?.tenCongViec || 'N/A' : '',
            trangThai: index === 0 ? statusLabels[collection.trangThai] || collection.trangThai : '',
            uuTien: index === 0 ? (collection.uuTien ? 'Gấp' : 'Bình thường') : '',
            nguoiGiaoLenh: index === 0 ? collection.nguoiGiaoLenh?.hoTen || 'N/A' : '',
            nhanVienThucHien: index === 0 ? collection.nhanVienThucHien?.hoTen || 'Chưa phân công' : '',
            khoangCach: index === 0 ? (collection.khoangCachDiChuyen ? collection.khoangCachDiChuyen.toFixed(2) : '-') : '',
            createdAt: index === 0 ? (collection.createdAt ? new Date(collection.createdAt).toLocaleString('vi-VN') : '') : '',
            thoiGianHenHoanThanh: index === 0 ? (collection.thoiGianHenHoanThanh
              ? new Date(collection.thoiGianHenHoanThanh).toLocaleString('vi-VN')
              : 'Không có') : '',
            ghiChu: index === 0 ? collection.ghiChu || '' : '',
            soTienCuocNhanMau: item.soTienCuocNhanMau || 0,
            soTienShip: item.soTienShip || 0,
            soTienGuiXe: item.soTienGuiXe || 0,
            tongTien: (item.soTienCuocNhanMau || 0) + (item.soTienShip || 0) + (item.soTienGuiXe || 0),
          });

          // Format money columns (now columns 13, 14, 15, 16 due to new nhaXe column)
          [13, 14, 15, 16].forEach((colNum) => {
            const cell = row.getCell(colNum);
            cell.numFmt = '#,##0';
          });

          // Highlight urgent orders
          if (collection.uuTien && index === 0) {
            row.getCell(6).fill = {
              type: 'pattern',
              pattern: 'solid',
              fgColor: { argb: 'FFFFCCCC' },
            };
          }
        });

        // Merge cells for bus station orders (columns that should be merged)
        const endRow = worksheet.rowCount;
        if (endRow > startRow) {
          // Merge: Mã lệnh, Nhà xe, Nội dung công việc, Trạng thái, Ưu tiên, Người giao lệnh, Nhân viên, Khoảng cách, Thời gian tạo, Hạn hoàn thành, Ghi chú
          [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12].forEach((colNum) => {
            worksheet.mergeCells(startRow, colNum, endRow, colNum);
            const cell = worksheet.getCell(startRow, colNum);
            cell.alignment = { vertical: 'middle', horizontal: 'center' };
          });
        }
      } else {
        // Regular clinic order (single row)
        let phongKhamName = '';
        if (collection.phongKhamItems && collection.phongKhamItems.length > 0) {
          const clinicNames = collection.phongKhamItems
            .map((item: any) => item.phongKham?.tenPhongKham)
            .filter((name: string) => name)
            .join(', ');
          phongKhamName = clinicNames || 'N/A';
        }

        // Calculate total money
        let tongTien = 0;
        if (collection.phongKhamItems && collection.phongKhamItems.length > 0) {
          collection.phongKhamItems.forEach((item: any) => {
            tongTien += (item.soTienCuocNhanMau || 0) + (item.soTienShip || 0) + (item.soTienGuiXe || 0);
          });
        }

        const row = worksheet.addRow({
          maLenh: collection.maLenh,
          nhaXe: '',
          phongKham: phongKhamName,
          noiDungCongViec: collection.noiDungCongViec?.tenCongViec || 'N/A',
          trangThai: statusLabels[collection.trangThai] || collection.trangThai,
          uuTien: collection.uuTien ? 'Gấp' : 'Bình thường',
          nguoiGiaoLenh: collection.nguoiGiaoLenh?.hoTen || 'N/A',
          nhanVienThucHien: collection.nhanVienThucHien?.hoTen || 'Chưa phân công',
          khoangCach: collection.khoangCachDiChuyen ? collection.khoangCachDiChuyen.toFixed(2) : '-',
          createdAt: collection.createdAt ? new Date(collection.createdAt).toLocaleString('vi-VN') : '',
          thoiGianHenHoanThanh: collection.thoiGianHenHoanThanh
            ? new Date(collection.thoiGianHenHoanThanh).toLocaleString('vi-VN')
            : 'Không có',
          ghiChu: collection.ghiChu || '',
          soTienCuocNhanMau:
            collection.phongKhamItems && collection.phongKhamItems.length > 0
              ? collection.phongKhamItems[0].soTienCuocNhanMau || 0
              : 0,
          soTienShip:
            collection.phongKhamItems && collection.phongKhamItems.length > 0
              ? collection.phongKhamItems[0].soTienShip || 0
              : 0,
          soTienGuiXe:
            collection.phongKhamItems && collection.phongKhamItems.length > 0
              ? collection.phongKhamItems[0].soTienGuiXe || 0
              : 0,
          tongTien: tongTien,
        });

        // Format money columns
        [13, 14, 15, 16].forEach((colNum) => {
          const cell = row.getCell(colNum);
          cell.numFmt = '#,##0';
        });

        // Highlight urgent orders
        if (collection.uuTien) {
          row.getCell(6).fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: 'FFFFCCCC' },
          };
        }
      }
    });

    // Auto-fit columns (approximate)
    worksheet.columns.forEach((column) => {
      if (column.width && column.width < 10) {
        column.width = 10;
      }
    });

    // Generate buffer
    const buffer = await workbook.xlsx.writeBuffer();
    return buffer;
  }

  // Lấy lịch sử tiến trình của lệnh
  async getHistory(sampleCollectionId: string) {
    return this.sampleCollectionHistoryModel
      .find({ sampleCollectionId })
      .sort({ thoiGian: 1 })
      .populate('nguoiThucHien', 'hoTen maNhanVien')
      .exec();
  }

  // Lưu lịch sử khi có thay đổi trạng thái
  async saveHistory(
    sampleCollectionId: string,
    trangThaiTruoc: SampleCollectionStatus | null,
    trangThai: SampleCollectionStatus,
    nguoiThucHien?: string,
    ghiChu?: string,
    duLieuThayDoi?: any
  ) {
    const history = new this.sampleCollectionHistoryModel({
      sampleCollectionId,
      trangThaiTruoc, // Trạng thái trước đó
      trangThai, // Trạng thái hiện tại
      nguoiThucHien,
      ghiChu,
      thoiGian: new Date(),
      duLieuThayDoi,
    });
    return history.save();
  }

  // Lấy tất cả lịch sử tiến trình (tất cả lệnh)
  async getAllHistory(limit: number = 500) {
    return this.sampleCollectionHistoryModel
      .find()
      .sort({ thoiGian: -1 })
      .limit(limit)
      .populate('sampleCollectionId', 'maLenh')
      .populate('nguoiThucHien', 'hoTen maNhanVien')
      .exec();
  }

  // Gửi lại email thông báo hoàn thành
  async resendCompletionEmail(id: string) {
    const collection = await this.sampleCollectionModel
      .findById(id)
      .populate('phongKhamItems.phongKham')
      .populate('nhanVienThucHien')
      .exec();

    if (!collection) {
      throw new Error('Không tìm thấy lệnh thu mẫu');
    }

    if (collection.trangThai !== SampleCollectionStatus.HOAN_THANH_KIEM_TRA) {
      throw new Error('Lệnh chưa hoàn thành kiểm tra');
    }

    if (!collection.phongKhamItems || collection.phongKhamItems.length === 0) {
      return {
        success: false,
        message: 'Không có phòng khám nào trong lệnh này',
      };
    }

    const employee = collection.nhanVienThucHien as any;
    const employeeName = employee?.hoTen || 'Nhân viên';

    let successCount = 0;
    let failCount = 0;

    for (const item of collection.phongKhamItems) {
      const clinic = item.phongKham as any;
      if (!clinic || !clinic.email) {
        failCount++;
        continue;
      }

      // Lấy ảnh hoàn thành kiểm tra của phòng khám này - chỉ lấy đường dẫn file
      const imagePaths = (item.anhHoanThanhKiemTra || []).map(url => {
        // Loại bỏ domain nếu có, chỉ giữ path
        const path = url.replace(/^https?:\/\/[^\/]+/, '');
        // Convert path thành đường dẫn file thực tế
        return path.startsWith('/') ? `.${path}` : `./${path}`;
      });

      const result = await this.emailService.sendCompletionEmail(
        clinic.email,
        clinic.tenPhongKham,
        collection.maLenh,
        new Date(),
        employeeName,
        imagePaths, // Truyền đường dẫn file local
      );

      if (result.success) {
        successCount++;
      } else {
        failCount++;
      }
    }

    // Nếu gửi thành công ít nhất 1 email, cập nhật emailSentAt
    if (successCount > 0) {
      await this.sampleCollectionModel.findByIdAndUpdate(id, {
        emailSentAt: new Date(),
      });
    }

    return {
      success: successCount > 0,
      message: `Đã gửi email đến ${successCount}/${collection.phongKhamItems.length} phòng khám`,
    };
  }
  async getDashboardStats(filters: {
    status?: string;
    employeeId?: string;
    startDate?: string;
    endDate?: string;
  }): Promise<any> {
    // Build query filter
    const query: any = {};

    if (filters.status) {
      query.trangThai = filters.status;
    }

    if (filters.employeeId) {
      query.nhanVienThucHien = filters.employeeId;
    }

    if (filters.startDate || filters.endDate) {
      query.createdAt = {};
      if (filters.startDate) {
        query.createdAt.$gte = new Date(filters.startDate);
      }
      if (filters.endDate) {
        const endDate = new Date(filters.endDate);
        endDate.setHours(23, 59, 59, 999); // End of day
        query.createdAt.$lte = endDate;
      }
    }

    // Fetch filtered collections
    const collections = await this.sampleCollectionModel
      .find(query)
      .populate('nhanVienThucHien', 'hoTen _id')
      .populate('phongKhamItems.phongKham')
      .exec();

    // Calculate summary stats từ phongKhamItems
    const tongSoLenh = collections.length;
    let tongTienCuocNhanMau = 0;
    let tongTienShip = 0;
    let tongTienGuiXe = 0;
    let tongKmDiDuoc = 0;

    // Tính số lệnh quá hạn
    const now = new Date();
    const overdueCollections = collections.filter(c => {
      return c.thoiGianHenHoanThanh && 
             new Date(c.thoiGianHenHoanThanh) < now &&
             c.trangThai !== SampleCollectionStatus.HOAN_THANH_KIEM_TRA &&
             c.trangThai !== SampleCollectionStatus.DA_HUY;
    });
    const tongSoLenhQuaHan = overdueCollections.length;

    // Tính tổng km dựa trên khoảng cách đến phòng khám
    // Giả định: mỗi lệnh = khoảng cách khứ hồi từ văn phòng đến phòng khám
    const officeLocation = { lat: 10.762622, lng: 106.660172 }; // Tọa độ văn phòng mặc định (TP.HCM)

    collections.forEach(c => {
      if (c.phongKhamItems && c.phongKhamItems.length > 0) {
        c.phongKhamItems.forEach(item => {
          tongTienCuocNhanMau += item.soTienCuocNhanMau || 0;
          tongTienShip += item.soTienShip || 0;
          tongTienGuiXe += item.soTienGuiXe || 0;
        });
      }
      
      // Cộng khoảng cách đã lưu (từ nhân viên đến phòng khám)
      if (c.khoangCachDiChuyen) {
        tongKmDiDuoc += c.khoangCachDiChuyen;
      }
    });

    // Calculate employee stats (số lệnh, km đi được, và số lệnh quá hạn)
    const employeeMap = new Map<string, { name: string; count: number; km: number; overdue: number }>();
    collections.forEach((c) => {
      if (c.nhanVienThucHien) {
        const employee = c.nhanVienThucHien as any;
        const id = employee._id.toString();
        const name = employee.hoTen;

        // Cộng km cho lệnh này (sử dụng khoảng cách đã lưu)
        let kmForOrder = c.khoangCachDiChuyen || 0;

        // Kiểm tra lệnh có quá hạn không
        const isOverdue = c.thoiGianHenHoanThanh && 
                         new Date(c.thoiGianHenHoanThanh) < now &&
                         c.trangThai !== SampleCollectionStatus.HOAN_THANH_KIEM_TRA &&
                         c.trangThai !== SampleCollectionStatus.DA_HUY;

        if (employeeMap.has(id)) {
          const existing = employeeMap.get(id)!;
          existing.count++;
          existing.km += kmForOrder;
          if (isOverdue) existing.overdue++;
        } else {
          employeeMap.set(id, { name, count: 1, km: kmForOrder, overdue: isOverdue ? 1 : 0 });
        }
      }
    });

    const employeeStats = Array.from(employeeMap.values()).map((v) => ({
      tenNhanVien: v.name,
      soLenh: v.count,
      kmDiDuoc: v.km,
      soLenhQuaHan: v.overdue,
    }));

    // Calculate status distribution
    const statusMap = new Map<string, number>();
    collections.forEach((c) => {
      const status = c.trangThai;
      statusMap.set(status, (statusMap.get(status) || 0) + 1);
    });

    const statusLabels: Record<string, string> = {
      CHO_DIEU_PHOI: 'Chờ điều phối',
      DANG_THUC_HIEN: 'Đang thực hiện',
      HOAN_THANH: 'Hoàn thành',
      HOAN_THANH_KIEM_TRA: 'Hoàn thành kiểm tra',
      DA_HUY: 'Đã hủy',
    };

    const statusDistribution = Array.from(statusMap.entries()).map(([status, count]) => ({
      trangThai: statusLabels[status] || status,
      soLuong: count,
      tiLe: tongSoLenh > 0 ? Math.round((count / tongSoLenh) * 100) : 0,
    }));

    return {
      summary: {
        tongSoLenh,
        tongTienCuocNhanMau,
        tongTienShip,
        tongTienGuiXe,
        tongTatCa: tongTienCuocNhanMau + tongTienShip + tongTienGuiXe,
        tongKmDiDuoc,
        tongSoLenhQuaHan,
      },
      employeeStats,
      statusDistribution,
    };
  }

  // Hàm tính khoảng cách Haversine
  private calculateDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const R = 6371; // Bán kính Trái Đất (km)
    const dLat = this.deg2rad(lat2 - lat1);
    const dLon = this.deg2rad(lon2 - lon1);
    const a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(this.deg2rad(lat1)) * Math.cos(this.deg2rad(lat2)) *
      Math.sin(dLon / 2) * Math.sin(dLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    const distance = R * c;
    return distance;
  }

  private deg2rad(deg: number): number {
    return deg * (Math.PI / 180);
  }

  // Tìm các lệnh quá hạn
  async findOverdueOrders(): Promise<SampleCollection[]> {
    const now = new Date();

    // Tìm các lệnh có:
    // - Hạn hoàn thành < hiện tại
    // - Trạng thái khác HOAN_THANH_KIEM_TRA và DA_HUY
    return this.sampleCollectionModel
      .find({
        thoiGianHenHoanThanh: { $lt: now },
        trangThai: {
          $nin: [
            SampleCollectionStatus.HOAN_THANH_KIEM_TRA,
            SampleCollectionStatus.DA_HUY
          ]
        },
      })
      .populate('phongKhamItems.phongKham')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .exec();
  }

  // Gửi thông báo cho các lệnh quá hạn
  async sendOverdueNotifications() {
    const overdueOrders = await this.findOverdueOrders();

    if (overdueOrders.length === 0) {
      return;
    }

    for (const order of overdueOrders) {
      // Kiểm tra xem đã gửi thông báo quá hạn chưa
      const existingNotification = await this.notificationService['notificationModel']
        .findOne({
          relatedOrderId: order._id.toString(),
          type: NotificationType.ORDER_OVERDUE,
        })
        .exec();

      // Nếu đã gửi thông báo quá hạn rồi thì bỏ qua
      if (existingNotification) {
        continue;
      }

      const recipientIds = new Set<string>();

      // 1. Người được giao
      if (order.nhanVienThucHien) {
        const nhanVienId = typeof order.nhanVienThucHien === 'object'
          ? (order.nhanVienThucHien as any)._id.toString()
          : (order.nhanVienThucHien as any).toString();
        recipientIds.add(nhanVienId);
      }

      // 2. Tất cả Admin
      const admins = await this.getAdminUsers();
      admins.forEach(admin => recipientIds.add(admin._id.toString()));

      // Lấy thông tin chi tiết cho thông báo
      let locationInfo = '';
      if (order.tenNhaXe) {
        locationInfo = ` tại nhà xe ${order.tenNhaXe}`;
      } else if (order.phongKhamItems && order.phongKhamItems.length > 0) {
        const firstClinic = order.phongKhamItems[0].phongKham as any;
        locationInfo = ` tại phòng khám ${firstClinic?.tenPhongKham || 'N/A'}`;
      }

      // Lấy tên nhân viên
      const employeeName = order.nhanVienThucHien
        ? (typeof order.nhanVienThucHien === 'object'
          ? (order.nhanVienThucHien as any).hoTen
          : 'nhân viên')
        : 'chưa giao';

      // Tính thời gian quá hạn
      const now = new Date();
      const deadline = new Date(order.thoiGianHenHoanThanh);
      const overdueHours = Math.floor((now.getTime() - deadline.getTime()) / (1000 * 60 * 60));
      const overdueMinutes = Math.floor(((now.getTime() - deadline.getTime()) % (1000 * 60 * 60)) / (1000 * 60));
      
      let overdueText = '';
      if (overdueHours > 0) {
        overdueText = `${overdueHours} giờ ${overdueMinutes} phút`;
      } else {
        overdueText = `${overdueMinutes} phút`;
      }

      // Trạng thái hiện tại
      const statusLabels: Record<string, string> = {
        CHO_DIEU_PHOI: 'Chờ điều phối',
        DANG_THUC_HIEN: 'Đang thực hiện',
        HOAN_THANH: 'Hoàn thành',
        HOAN_THANH_KIEM_TRA: 'Hoàn thành kiểm tra',
        DA_HUY: 'Đã hủy',
      };
      const currentStatus = statusLabels[order.trangThai] || order.trangThai;

      // Gửi thông báo cho tất cả người nhận với thông tin chi tiết
      for (const userId of recipientIds) {
        await this.notificationService.create({
          userId,
          title: '⚠️ Lệnh thu mẫu quá hạn',
          message: `Lệnh ${order.maLenh}${locationInfo} đã quá hạn ${overdueText}. Nhân viên: ${employeeName}. Trạng thái: ${currentStatus}`,
          type: NotificationType.ORDER_OVERDUE,
          relatedOrderId: order._id.toString(),
        });
      }
    }
  }

  // Cron job chạy mỗi giờ để kiểm tra lệnh quá hạn
  @Cron(CronExpression.EVERY_HOUR)
  async handleOverdueCheck() {
    await this.sendOverdueNotifications();
  }

  // Cron job tự động tạo lệnh mỗi ngày lúc 8:00 sáng
  @Cron('0 8 * * *')
  async handleAutoCreateOrders() {
    try {
      // Lấy admin đầu tiên làm người giao lệnh
      const admins = await this.getAdminUsers();
      if (admins.length === 0) {
        console.error('No admin users found for auto-create orders');
        return;
      }

      const nguoiGiaoLenh = admins[0]._id.toString();
      const result = await this.autoCreateOrders(nguoiGiaoLenh);

      if (result.errors.length > 0) {
        console.error('Auto-create orders errors:', result.errors);
      }
    } catch (error) {
      console.error('Error in auto-create orders cron job:', error);
    }
  }

  // Tự động tạo lệnh thu mẫu dựa trên cấu hình phòng khám
  async autoCreateOrders(nguoiGiaoLenh: string): Promise<{ created: number; skipped: number; errors: string[] }> {
    const today = new Date();
    const dayOfWeek = today.getDay(); // 0=CN, 1=T2, ..., 6=T7
    const dateStr = today.toISOString().split('T')[0]; // YYYY-MM-DD
    const clinics = await this.sampleCollectionModel.db.collection('clinics').find({
      tuDongTaoLenh: true,
      dangHoatDong: true,
    }).toArray();

    let created = 0;
    let skipped = 0;
    const errors: string[] = [];

    for (const clinic of clinics) {
      try {
        // Kiểm tra phòng khám có mở cửa hôm nay không (dựa trên gioLamViec mới)
        let isClinicOpen = false;
        if (clinic.gioLamViec && Array.isArray(clinic.gioLamViec)) {
          const todaySchedule = clinic.gioLamViec.find((schedule: any) => schedule.day === dayOfWeek);
          isClinicOpen = todaySchedule?.isOpen === true;
        } else if (clinic.ngayLamViec && Array.isArray(clinic.ngayLamViec)) {
          // Fallback to old ngayLamViec field for backward compatibility
          isClinicOpen = clinic.ngayLamViec.includes(dayOfWeek);
        }

        if (!isClinicOpen) {
          skipped++;
          continue;
        }

        // Kiểm tra xem có cấu hình nào khớp với ngày hôm nay không
        const cauHinhList = clinic.cauHinhTuDongTaoLenh || [];

        for (const cauHinh of cauHinhList) {
          // Kiểm tra ngày trong tuần có khớp không
          if (!cauHinh.ngayTaoLenhTrongTuan || !cauHinh.ngayTaoLenhTrongTuan.includes(dayOfWeek)) {
            continue;
          }

          // Kiểm tra xem hôm nay đã tạo lệnh cho cấu hình này chưa
          const existingOrder = await this.sampleCollectionModel.findOne({
            'phongKhamItems.phongKham': clinic._id,
            noiDungCongViec: cauHinh.noiDungCongViecMacDinh,
            createdAt: {
              $gte: new Date(dateStr + 'T00:00:00.000Z'),
              $lt: new Date(dateStr + 'T23:59:59.999Z'),
            },
          }).exec();

          if (existingOrder) {
            skipped++;
            continue;
          }
          // Tính thời gian hẹn hoàn thành: 2 giờ sau khi tạo (10:00 sáng nếu tạo lúc 8:00 sáng)
          const thoiGianHenHoanThanh = new Date(Date.now() + 2 * 60 * 60 * 1000);

          const orderData = {
            noiDungCongViec: cauHinh.noiDungCongViecMacDinh,
            nguoiGiaoLenh,
            ghiChu: cauHinh.ghiChuLenh || '',
            uuTien: cauHinh.lenhUuTien || false,
            trangThai: SampleCollectionStatus.CHO_DIEU_PHOI,
            thoiGianHenHoanThanh,
            phongKhamItems: [{
              phongKham: clinic._id.toString(),
              soTienCuocNhanMau: 0,
              soTienShip: 0,
              soTienGuiXe: 0,
              anhHoanThanhKiemTra: []
            }]
          };

          await this.create(orderData);
          created++;
        }
      } catch (error) {
        errors.push(`${clinic.tenPhongKham}: ${error.message}`);
      }
    }

    return { created, skipped, errors };
  }

  // Helper method để kiểm tra xem lệnh có phải là bus station order không
  private isBusStationOrder(order: SampleCollection): boolean {
    const workContentId = typeof order.noiDungCongViec === 'object'
      ? (order.noiDungCongViec as any)._id.toString()
      : String(order.noiDungCongViec);
    return workContentId === '698a9efbbb8b349dfb294dd0';
  }

  // Hoàn thành lệnh bus station (chỉ upload ảnh, không cần nhập tiền)
  async completeBusStationOrder(
    id: string,
    anhHoanThanh: string[],
    nguoiThucHien: string
  ): Promise<SampleCollection> {
    const order = await this.findOne(id);

    if (!order) {
      throw new Error('Không tìm thấy lệnh thu mẫu');
    }

    if (!this.isBusStationOrder(order)) {
      throw new Error('Lệnh này không phải là lệnh nhận mẫu từ nhà xe');
    }

    // Cập nhật trạng thái sang HOAN_THANH với ảnh
    return this.updateStatus(id, SampleCollectionStatus.HOAN_THANH, {
      anhHoanThanh,
      thoiGianHoanThanh: new Date(),
      nguoiThucHien
    });
  }

  // Hoàn thành kiểm tra với nhiều phòng khám (cho bus station order)
  async completeVerificationWithClinicItems(
    id: string,
    phongKhamItems: Array<{
      phongKham: string;
      soTienCuocNhanMau: number;
      soTienShip: number;
      soTienGuiXe: number;
      anhHoanThanhKiemTra: string[];
    }>,
    nguoiThucHien: string
  ): Promise<SampleCollection> {
    const order = await this.findOne(id);

    if (!order) {
      throw new Error('Không tìm thấy lệnh thu mẫu');
    }

    if (!this.isBusStationOrder(order)) {
      throw new Error('Lệnh này không phải là lệnh nhận mẫu từ nhà xe');
    }

    if (order.trangThai !== SampleCollectionStatus.HOAN_THANH) {
      throw new Error('Lệnh phải ở trạng thái HOAN_THANH trước khi hoàn thành kiểm tra');
    }

    if (!phongKhamItems || phongKhamItems.length === 0) {
      throw new Error('Phải có ít nhất một phòng khám');
    }

    // Cập nhật phongKhamItems và chuyển sang HOAN_THANH_KIEM_TRA
    const updated = await this.sampleCollectionModel
      .findByIdAndUpdate(
        id,
        {
          phongKhamItems,
          trangThai: SampleCollectionStatus.HOAN_THANH_KIEM_TRA,
          thoiGianHoanThanhKiemTra: new Date()
        },
        { new: true }
      )
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .exec();

    // Không tự động gửi email - chỉ gửi khi người dùng click nút "Gửi lại email"
    // await this.sendBusStationClinicEmails(updated);

    // Lưu lịch sử
    await this.saveHistory(
      id,
      SampleCollectionStatus.HOAN_THANH,
      SampleCollectionStatus.HOAN_THANH_KIEM_TRA,
      nguoiThucHien,
      'Hoàn thành kiểm tra với nhiều phòng khám'
    );

    // Gửi thông báo
    await this.sendStatusChangeNotifications(updated, 'Hoàn thành kiểm tra');

    return updated;
  }

  // Hoàn thành kiểm tra cho lệnh thu mẫu thường
  async completeVerification(
    id: string,
    anhHoanThanhKiemTra: string[],
    nguoiThucHien: string
  ): Promise<SampleCollection> {
    const order = await this.findOne(id);

    if (!order) {
      throw new Error('Không tìm thấy lệnh thu mẫu');
    }

    if (this.isBusStationOrder(order)) {
      throw new Error('Lệnh nhà xe phải sử dụng endpoint khác');
    }

    if (order.trangThai !== SampleCollectionStatus.HOAN_THANH) {
      throw new Error('Lệnh phải ở trạng thái HOAN_THANH trước khi hoàn thành kiểm tra');
    }

    // Ensure phongKhamItems exists and has at least one element
    if (!order.phongKhamItems || order.phongKhamItems.length === 0) {
      throw new Error('Lệnh không có thông tin phòng khám');
    }

    // Get existing verification images and append new ones
    const existingImages = order.phongKhamItems[0]?.anhHoanThanhKiemTra || [];
    const combinedImages = [...existingImages, ...anhHoanThanhKiemTra];

    // Update the first phongKhamItems element directly using MongoDB's array update syntax
    const updated = await this.sampleCollectionModel
      .findByIdAndUpdate(
        id,
        {
          $set: {
            'phongKhamItems.0.anhHoanThanhKiemTra': combinedImages, // APPEND instead of REPLACE
            'phongKhamItems.0.thoiGianHoanThanhKiemTra': new Date(),
            trangThai: SampleCollectionStatus.HOAN_THANH_KIEM_TRA,
            thoiGianHoanThanhKiemTra: new Date()
          }
        },
        { new: true }
      )
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .exec();

    if (!updated) {
      throw new Error('Không thể cập nhật lệnh thu mẫu');
    }

    // Lưu lịch sử
    await this.saveHistory(
      id,
      SampleCollectionStatus.HOAN_THANH,
      SampleCollectionStatus.HOAN_THANH_KIEM_TRA,
      nguoiThucHien,
      'Hoàn thành kiểm tra'
    );

    // Gửi thông báo
    await this.sendStatusChangeNotifications(updated, 'Hoàn thành kiểm tra');

    return updated;
  }

  // Gửi email cho các phòng khám trong bus station order
  private async sendBusStationClinicEmails(order: SampleCollection): Promise<void> {
    if (!order.phongKhamItems || order.phongKhamItems.length === 0) {
      return;
    }

    const employee = order.nhanVienThucHien as any;
    const employeeName = employee?.hoTen || 'Nhân viên';

    for (const item of order.phongKhamItems) {
      try {
        const clinic = item.phongKham as any;
        if (!clinic || !clinic.email) {
          console.warn(`Clinic ${clinic?._id} has no email configured`);
          continue;
        }

        await this.emailService.sendBusStationCompletionEmail(
          clinic.email,
          clinic.tenPhongKham,
          order.maLenh,
          order.tenNhaXe,
          order.diaChiNhaXe,
          item.soTienCuocNhanMau,
          item.soTienShip,
          item.soTienGuiXe,
          item.anhHoanThanhKiemTra,
          new Date(),
          employeeName
        );
      } catch (error) {
        console.error(`Failed to send email for clinic ${item.phongKham}:`, error);
        // Continue with other clinics even if one fails
      }
    }
  }

  async getMessages(sampleCollectionId: string): Promise<SampleCollectionMessage[]> {
    return this.sampleCollectionMessageModel
      .find({ sampleCollectionId })
      .populate('userId', 'hoTen email')
      .sort({ createdAt: 1 })
      .exec();
  }

  async sendMessage(sampleCollectionId: string, userId: string, message: string): Promise<SampleCollectionMessage> {
    const newMessage = new this.sampleCollectionMessageModel({
      sampleCollectionId,
      userId,
      message,
    });
    return newMessage.save();
  }

  async updateVerificationImages(id: string, phongKhamItems: any[]): Promise<SampleCollection> {
    const order = await this.findOne(id);

    if (!order) {
      throw new Error('Không tìm thấy lệnh thu mẫu');
    }

    // Update the order with new phongKhamItems
    const updated = await this.sampleCollectionModel
      .findByIdAndUpdate(
        id,
        { phongKhamItems },
        { new: true }
      )
      .populate('phongKhamItems.phongKham')
      .populate('noiDungCongViec')
      .populate('nguoiGiaoLenh')
      .populate('nhanVienThucHien')
      .populate('phongKhamKiemTra')
      .exec();

    if (!updated) {
      throw new Error('Không thể cập nhật lệnh thu mẫu');
    }

    return updated;
  }
}
