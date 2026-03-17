import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model, Types } from 'mongoose';
import * as XLSX from 'xlsx';
import { Clinic } from '../clinic/schemas/clinic.schema';
import { AdjustStockDto } from './dto/adjust-stock.dto';
import { ConfirmDeliveryDto } from './dto/confirm-delivery.dto';
import { CreateAllocationDto } from './dto/create-allocation.dto';
import { CreateSupplyDto } from './dto/create-supply.dto';
import { PrepareAllocationDto } from './dto/prepare-allocation.dto';
import { UpdateSupplyDto } from './dto/update-supply.dto';
import { AllocationStatus, DeliveryMethod, SupplyAllocation, SupplyAllocationDocument } from './schemas/supply-allocation.schema';
import { HistoryType, SupplyHistory, SupplyHistoryDocument } from './schemas/supply-history.schema';
import { Supply, SupplyDocument, SupplyStatus } from './schemas/supply.schema';

@Injectable()
export class SupplyService {
  constructor(
    @InjectModel(Supply.name)
    private supplyModel: Model<SupplyDocument>,
    @InjectModel(SupplyAllocation.name)
    private allocationModel: Model<SupplyAllocationDocument>,
    @InjectModel(SupplyHistory.name)
    private historyModel: Model<SupplyHistoryDocument>,
    @InjectModel(Clinic.name)
    private clinicModel: Model<Clinic>,
  ) { }

  // ============ QUẢN LÝ VẬT TƯ ============

  async createSupply(createSupplyDto: CreateSupplyDto): Promise<SupplyDocument> {
    // Auto-generate maVatTu if not provided
    if (!createSupplyDto.maVatTu) {
      // Find the highest existing supply code
      const lastSupply = await this.supplyModel
        .findOne({ maVatTu: /^VT\d+$/ })
        .sort({ maVatTu: -1 })
        .exec();

      let nextNumber = 1;
      if (lastSupply && lastSupply.maVatTu) {
        const lastNumber = parseInt(lastSupply.maVatTu.replace('VT', ''));
        nextNumber = lastNumber + 1;
      }

      createSupplyDto.maVatTu = `VT${String(nextNumber).padStart(4, '0')}`;
    }

    const supply = new this.supplyModel(createSupplyDto);
    return supply.save();
  }

  async findAllSupplies(): Promise<SupplyDocument[]> {
    return this.supplyModel.find().sort({ maVatTu: 1 }).exec();
  }

  async findAllSuppliesWithPagination(params?: {
    status?: string;
    search?: string;
    page?: number;
    limit?: number;
  }): Promise<{
    data: SupplyDocument[];
    total: number;
    page: number;
    limit: number;
    totalPages: number;
  }> {
    const { status, search, page = 1, limit = 10 } = params || {};

    const filter: any = {};

    if (status) {
      filter.trangThai = status;
    }

    if (search) {
      filter.$or = [
        { maVatTu: { $regex: search, $options: 'i' } },
        { tenVatTu: { $regex: search, $options: 'i' } },
        { moTa: { $regex: search, $options: 'i' } },
      ];
    }

    const skip = (page - 1) * limit;
    const total = await this.supplyModel.countDocuments(filter).exec();

    const data = await this.supplyModel
      .find(filter)
      .sort({ maVatTu: 1 })
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

  async findSupplyById(id: string): Promise<SupplyDocument> {
    const supply = await this.supplyModel.findById(id).exec();
    if (!supply) {
      throw new NotFoundException('Không tìm thấy vật tư');
    }
    return supply;
  }

  async updateSupply(id: string, updateSupplyDto: UpdateSupplyDto): Promise<SupplyDocument> {
    const supply = await this.supplyModel
      .findByIdAndUpdate(id, updateSupplyDto, { new: true })
      .exec();

    if (!supply) {
      throw new NotFoundException('Không tìm thấy vật tư');
    }

    // Cập nhật trạng thái dựa trên tồn kho
    await this.updateSupplyStatus(id);

    return supply;
  }

  async deleteSupply(id: string): Promise<void> {
    const result = await this.supplyModel.findByIdAndDelete(id).exec();
    if (!result) {
      throw new NotFoundException('Không tìm thấy vật tư');
    }
  }

  // Điều chỉnh tồn kho
  async adjustStock(id: string, adjustStockDto: AdjustStockDto): Promise<SupplyDocument> {
    const supply = await this.findSupplyById(id);

    const newStock = supply.tonKho + adjustStockDto.soLuong;
    if (newStock < 0) {
      throw new BadRequestException('Số lượng tồn kho không thể âm');
    }

    supply.tonKho = newStock;
    await supply.save();

    // Lưu lịch sử
    await this.saveHistory({
      vatTu: supply._id,
      loaiThayDoi: HistoryType.DIEU_CHINH,
      soLuong: adjustStockDto.soLuong,
      lyDo: adjustStockDto.lyDo || 'Điều chỉnh kho',
      nguoiThucHien: adjustStockDto.nguoiThucHien,
      thoiGian: new Date(),
    });

    // Cập nhật trạng thái
    await this.updateSupplyStatus(id);

    return supply;
  }

  // Cập nhật trạng thái vật tư dựa trên tồn kho
  private async updateSupplyStatus(id: string): Promise<void> {
    const supply = await this.findSupplyById(id);

    const newStatus = supply.tonKho < supply.mucToiThieu
      ? SupplyStatus.CAN_NHAP_THEM
      : SupplyStatus.BINH_THUONG;

    if (supply.trangThai !== newStatus) {
      supply.trangThai = newStatus;
      await supply.save();
    }
  }

  // ============ QUẢN LÝ PHIẾU CẤP PHÁT ============

  async createAllocation(createAllocationDto: CreateAllocationDto): Promise<SupplyAllocationDocument> {
    // Generate mã phiếu: PC + ddMMyyHHmmss + random 3 digits to prevent duplicates
    const now = new Date();
    const day = String(now.getDate()).padStart(2, '0');
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const year = String(now.getFullYear()).slice(-2);
    const hour = String(now.getHours()).padStart(2, '0');
    const minute = String(now.getMinutes()).padStart(2, '0');
    const second = String(now.getSeconds()).padStart(2, '0');
    const random = String(Math.floor(Math.random() * 1000)).padStart(3, '0');

    const maPhieu = `PC${day}${month}${year}${hour}${minute}${second}${random}`;

    // Kiểm tra tồn kho đủ không
    for (const item of createAllocationDto.danhSachVatTu) {
      const supply = await this.findSupplyById(item.vatTu);
      if (supply.tonKho < item.soLuong) {
        throw new BadRequestException(
          `Vật tư "${supply.tenVatTu}" không đủ số lượng. Tồn kho: ${supply.tonKho}, Yêu cầu: ${item.soLuong}`
        );
      }
    }

    const allocation = new this.allocationModel({
      ...createAllocationDto,
      maPhieu,
      trangThai: AllocationStatus.CHO_CHUAN_BI,
    });

    return allocation.save();
  }

  async findAllAllocations(params?: {
    status?: string;
    search?: string;
    page?: number;
    limit?: number;
  }): Promise<{
    data: SupplyAllocationDocument[];
    total: number;
    page: number;
    limit: number;
    totalPages: number;
  }> {
    const { status, search, page = 1, limit = 10 } = params || {};

    const filter: any = {};

    if (status) {
      filter.trangThai = status;
    }

    // Search by maPhieu or clinic name
    if (search) {
      // First, find clinics matching the search term
      const clinics = await this.allocationModel.db.collection('clinics').find({
        tenPhongKham: { $regex: search, $options: 'i' }
      }).toArray();

      const clinicIds = clinics.map(c => c._id);

      // Search by maPhieu OR clinic name
      filter.$or = [
        { maPhieu: { $regex: search, $options: 'i' } },
        { phongKham: { $in: clinicIds } }
      ];
    }

    const skip = (page - 1) * limit;
    const total = await this.allocationModel.countDocuments(filter).exec();

    const data = await this.allocationModel
      .find(filter)
      .populate('nguoiTaoPhieu', 'hoTen')
      .populate('phongKham', 'maPhongKham tenPhongKham')
      .populate('nguoiGiaoHang', 'hoTen')
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit)
      .exec();

    // Populate maVatTu for each supply item in danhSachVatTu
    for (const allocation of data) {
      for (const item of allocation.danhSachVatTu) {
        const supply = await this.supplyModel.findById(item.vatTu).select('maVatTu').exec();
        if (supply) {
          (item as any).maVatTu = supply.maVatTu;
        }
      }
    }

    return {
      data,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findAllocationById(id: string): Promise<SupplyAllocationDocument> {
    const allocation = await this.allocationModel
      .findById(id)
      .populate('nguoiTaoPhieu', 'hoTen')
      .populate('phongKham')
      .populate('nguoiGiaoHang', 'hoTen')
      .exec();

    if (!allocation) {
      throw new NotFoundException('Không tìm thấy phiếu cấp phát');
    }

    return allocation;
  }

  // Cập nhật phiếu cấp phát (chỉ cho phép khi ở trạng thái CHO_CHUAN_BI)
  async updateAllocation(id: string, updateData: Partial<CreateAllocationDto>): Promise<SupplyAllocationDocument> {
    const allocation = await this.findAllocationById(id);

    // Chỉ cho phép sửa khi phiếu ở trạng thái chờ chuẩn bị
    if (allocation.trangThai !== AllocationStatus.CHO_CHUAN_BI) {
      throw new BadRequestException('Chỉ có thể sửa phiếu ở trạng thái chờ chuẩn bị');
    }

    // Nếu có thay đổi danh sách vật tư, kiểm tra tồn kho
    if (updateData.danhSachVatTu) {
      for (const item of updateData.danhSachVatTu) {
        const supply = await this.findSupplyById(item.vatTu);
        if (supply.tonKho < item.soLuong) {
          throw new BadRequestException(
            `Vật tư "${supply.tenVatTu}" không đủ số lượng. Tồn kho: ${supply.tonKho}, Yêu cầu: ${item.soLuong}`
          );
        }
      }
    }

    // Cập nhật thông tin
    Object.assign(allocation, updateData);
    return allocation.save();
  }

  // Chuẩn bị hàng - Cập nhật hạn sử dụng và trừ tồn kho
  async prepareAllocation(id: string, prepareAllocationDto: PrepareAllocationDto): Promise<SupplyAllocationDocument> {
    const allocation = await this.findAllocationById(id);

    if (allocation.trangThai !== AllocationStatus.CHO_CHUAN_BI) {
      throw new BadRequestException('Phiếu không ở trạng thái chờ chuẩn bị');
    }

    // Cập nhật hạn sử dụng cho từng vật tư
    for (const expiryItem of prepareAllocationDto.danhSachHanSuDung) {
      const supplyItem = allocation.danhSachVatTu.find(
        item => item.vatTu.toString() === expiryItem.vatTu
      );

      if (supplyItem) {
        supplyItem.hanSuDung = new Date(expiryItem.hanSuDung);
      }
    }

    // Trừ tồn kho
    for (const item of allocation.danhSachVatTu) {
      const supply = await this.findSupplyById(item.vatTu.toString());

      if (supply.tonKho < item.soLuong) {
        throw new BadRequestException(
          `Vật tư "${supply.tenVatTu}" không đủ số lượng`
        );
      }

      supply.tonKho -= item.soLuong;
      await supply.save();

      // Lưu lịch sử xuất kho
      await this.saveHistory({
        vatTu: supply._id,
        loaiThayDoi: HistoryType.XUAT_CAP,
        soLuong: -item.soLuong,
        lyDo: `Xuất cấp cho ${(allocation.phongKham as any).tenPhongKham} - Mã phiếu: ${allocation.maPhieu}`,
        nguoiThucHien: allocation.nguoiTaoPhieu,
        phieuCapPhat: allocation._id,
        thoiGian: new Date(),
      });

      // Cập nhật trạng thái vật tư
      await this.updateSupplyStatus(supply._id.toString());
    }

    allocation.trangThai = AllocationStatus.CHUAN_BI_HANG;
    return allocation.save();
  }

  // Xác nhận đã giao
  async confirmDelivery(id: string, confirmDeliveryDto: ConfirmDeliveryDto): Promise<SupplyAllocationDocument> {
    const allocation = await this.findAllocationById(id);

    if (allocation.trangThai !== AllocationStatus.CHUAN_BI_HANG) {
      throw new BadRequestException('Phiếu không ở trạng thái chuẩn bị hàng');
    }

    allocation.trangThai = AllocationStatus.DA_GIAO;
    allocation.ngayGiao = new Date(confirmDeliveryDto.ngayGiao);
    allocation.ghiChu = confirmDeliveryDto.ghiChu;
    allocation.anhGiaoNhan = confirmDeliveryDto.anhGiaoNhan;
    allocation.nguoiGiaoHang = confirmDeliveryDto.nguoiGiaoHang as any;
    allocation.thoiGianGiao = new Date();

    return allocation.save();
  }

  // Xóa phiếu và hoàn kho
  async deleteAllocation(id: string): Promise<void> {
    const allocation = await this.findAllocationById(id);

    // Nếu đã chuẩn bị hàng hoặc đã giao, hoàn lại kho
    if (allocation.trangThai === AllocationStatus.CHUAN_BI_HANG ||
      allocation.trangThai === AllocationStatus.DA_GIAO) {

      for (const item of allocation.danhSachVatTu) {
        const supply = await this.findSupplyById(item.vatTu.toString());
        supply.tonKho += item.soLuong;
        await supply.save();

        // Lưu lịch sử hoàn kho
        await this.saveHistory({
          vatTu: supply._id,
          loaiThayDoi: HistoryType.HOAN_KHO,
          soLuong: item.soLuong,
          lyDo: `Hoàn kho do xóa phiếu ${allocation.maPhieu}`,
          nguoiThucHien: allocation.nguoiTaoPhieu,
          phieuCapPhat: allocation._id,
          thoiGian: new Date(),
        });

        // Cập nhật trạng thái vật tư
        await this.updateSupplyStatus(supply._id.toString());
      }
    }

    await this.allocationModel.findByIdAndDelete(id).exec();
  }

  // Cập nhật trạng thái gửi Zalo
  async markZaloSent(id: string): Promise<SupplyAllocationDocument> {
    const allocation = await this.allocationModel
      .findByIdAndUpdate(id, { daGuiZalo: true }, { new: true })
      .exec();

    if (!allocation) {
      throw new NotFoundException('Không tìm thấy phiếu cấp phát');
    }

    return allocation;
  }

  // ============ LỊCH SỬ ============

  async getSupplyHistory(supplyId: string, params?: {
    startDate?: string;
    endDate?: string;
  }): Promise<SupplyHistoryDocument[]> {
    // Convert string to ObjectId for proper MongoDB query
    const objectId = new Types.ObjectId(supplyId);

    const filter: any = {
      vatTu: objectId,
      loaiThayDoi: { $ne: HistoryType.NHAN_MAU_VE }
    };

    if (params?.startDate || params?.endDate) {
      filter.thoiGian = {};
      if (params.startDate) {
        filter.thoiGian.$gte = new Date(params.startDate);
      }
      if (params.endDate) {
        const endDate = new Date(params.endDate);
        endDate.setHours(23, 59, 59, 999);
        filter.thoiGian.$lte = endDate;
      }
    }

    const history = await this.historyModel
      .find(filter)
      .populate('nguoiThucHien', 'hoTen')
      .populate('phieuCapPhat', 'maPhieu')
      .sort({ thoiGian: -1 })
      .exec();

    return history;
  }

  private async saveHistory(data: any): Promise<SupplyHistoryDocument> {
    const history = new this.historyModel(data);
    return history.save();
  }

  // ============ EXCEL IMPORT/EXPORT ============

  async generateExcelTemplate(): Promise<any> {
    // Get all supplies and clinics for reference
    const supplies = await this.supplyModel.find().select('maVatTu tenVatTu').exec();
    const clinics = await this.allocationModel.db.collection('clinics').find().toArray();

    // Create workbook
    const wb = XLSX.utils.book_new();

    // Sheet 1: Template for data entry
    const templateData = [
      ['Mã PK', 'Mã VT', 'Số lượng', 'Hình thức vận chuyển'],
      ['PK001', 'VT001', 10, 'CAP_TAN_NOI'],
      ['PK001', 'VT002', 5, 'GUI_CHUYEN_PHAT'],
    ];
    const ws1 = XLSX.utils.aoa_to_sheet(templateData);
    XLSX.utils.book_append_sheet(wb, ws1, 'Nhập liệu');

    // Sheet 2: Clinic reference
    const clinicData = [['Mã PK', 'Tên phòng khám']];
    clinics.forEach((clinic: any) => {
      clinicData.push([clinic.maPhongKham, clinic.tenPhongKham]);
    });
    const ws2 = XLSX.utils.aoa_to_sheet(clinicData);
    XLSX.utils.book_append_sheet(wb, ws2, 'Danh sách PK');

    // Sheet 3: Supply reference
    const supplyData = [['Mã VT', 'Tên vật tư', 'Tồn kho']];
    supplies.forEach((supply: any) => {
      supplyData.push([supply.maVatTu, supply.tenVatTu, supply.tonKho]);
    });
    const ws3 = XLSX.utils.aoa_to_sheet(supplyData);
    XLSX.utils.book_append_sheet(wb, ws3, 'Danh sách VT');

    // Sheet 4: Instructions
    const instructions = [
      ['HƯỚNG DẪN NHẬP PHIẾU CẤP PHÁT VẬT TƯ'],
      [''],
      ['1. Mã PK: Nhập mã phòng khám (xem sheet "Danh sách PK")'],
      ['2. Mã VT: Nhập mã vật tư (xem sheet "Danh sách VT")'],
      ['3. Số lượng: Nhập số lượng cần cấp (số nguyên dương)'],
      ['4. Hình thức vận chuyển: Chọn một trong các giá trị sau:'],
      ['   - CAP_TAN_NOI: Cấp tận nơi'],
      ['   - GUI_CHUYEN_PHAT: Gửi chuyển phát'],
      ['   - GUI_XE_SHIP: Gửi xe, Ship'],
      [''],
      ['LƯU Ý:'],
      ['- Mỗi dòng là một vật tư cấp cho một phòng khám'],
      ['- Nếu cùng phòng khám nhận nhiều vật tư, ghi nhiều dòng với cùng Mã PK'],
      ['- Hệ thống sẽ tự động gộp các dòng cùng Mã PK thành một phiếu'],
      ['- Kiểm tra tồn kho trước khi nhập để tránh lỗi'],
    ];
    const ws4 = XLSX.utils.aoa_to_sheet(instructions);
    XLSX.utils.book_append_sheet(wb, ws4, 'Hướng dẫn');

    // Generate buffer
    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });

    return {
      buffer,
      filename: `Mau_Nhap_Cap_Phat_${new Date().toISOString().split('T')[0]}.xlsx`,
    };
  }

  async importAllocationsFromExcel(file: Express.Multer.File, nguoiTaoPhieu: string): Promise<any> {
    if (!file) {
      throw new BadRequestException('Không có file được tải lên');
    }

    try {
      // Read Excel file
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];

      if (!sheetName) {
        throw new BadRequestException('File Excel không có sheet nào');
      }

      const worksheet = workbook.Sheets[sheetName];
      const data: any[] = XLSX.utils.sheet_to_json(worksheet);

      if (data.length === 0) {
        throw new BadRequestException('File Excel không có dữ liệu');
      }

      // Validate and group by clinic
      const groupedByClinic: Map<string, any[]> = new Map();
      const errors: string[] = [];

      for (let i = 0; i < data.length; i++) {
        const row = data[i];
        const rowNum = i + 2; // Excel row number (header is row 1)

        // Skip empty rows
        if (!row['Mã PK'] && !row['Mã VT'] && !row['Số lượng'] && !row['Hình thức vận chuyển']) {
          continue;
        }

        // Validate required fields
        if (!row['Mã PK']) {
          errors.push(`Dòng ${rowNum}: Thiếu Mã PK`);
          continue;
        }
        if (!row['Mã VT']) {
          errors.push(`Dòng ${rowNum}: Thiếu Mã VT`);
          continue;
        }
        const quantity = Number(row['Số lượng']);
        if (!row['Số lượng'] || isNaN(quantity) || quantity <= 0 || !Number.isInteger(quantity)) {
          errors.push(`Dòng ${rowNum}: Số lượng không hợp lệ (phải là số nguyên dương)`);
          continue;
        }
        if (!row['Hình thức vận chuyển']) {
          errors.push(`Dòng ${rowNum}: Thiếu hình thức vận chuyển`);
          continue;
        }

        // Validate delivery method
        const validMethods = ['CAP_TAN_NOI', 'GUI_CHUYEN_PHAT', 'GUI_XE_SHIP'];
        if (!validMethods.includes(row['Hình thức vận chuyển'])) {
          errors.push(`Dòng ${rowNum}: Hình thức vận chuyển không hợp lệ`);
          continue;
        }

        // Find clinic
        const clinic = await this.allocationModel.db.collection('clinics').findOne({
          maPhongKham: row['Mã PK'],
        });
        if (!clinic) {
          errors.push(`Dòng ${rowNum}: Không tìm thấy phòng khám với mã "${row['Mã PK']}"`);
          continue;
        }

        // Find supply
        const supply = await this.supplyModel.findOne({ maVatTu: row['Mã VT'] }).exec();
        if (!supply) {
          errors.push(`Dòng ${rowNum}: Không tìm thấy vật tư với mã "${row['Mã VT']}"`);
          continue;
        }

        // Group by clinic and delivery method
        const clinicKey = `${clinic._id}_${row['Hình thức vận chuyển']}`;
        if (!groupedByClinic.has(clinicKey)) {
          groupedByClinic.set(clinicKey, []);
        }

        groupedByClinic.get(clinicKey)!.push({
          vatTu: supply._id.toString(),
          tenVatTu: supply.tenVatTu,
          soLuong: Number(row['Số lượng']),
          phongKham: clinic._id,
          hinhThucVanChuyen: row['Hình thức vận chuyển'],
          tonKho: supply.tonKho, // Store current stock for validation later
        });
      }

      if (errors.length > 0) {
        throw new BadRequestException({
          message: 'Có lỗi trong file Excel',
          errors,
        });
      }

      // Validate stock BEFORE creating any allocations
      // Track total usage across ALL allocations to prevent overselling
      const totalSupplyUsage: Map<string, number> = new Map();

      for (const [, items] of groupedByClinic.entries()) {
        for (const item of items) {
          const currentUsage = totalSupplyUsage.get(item.vatTu) || 0;
          totalSupplyUsage.set(item.vatTu, currentUsage + item.soLuong);
        }
      }

      // Check if we have enough stock for ALL allocations combined
      for (const [supplyId, totalQty] of totalSupplyUsage.entries()) {
        const supply = await this.findSupplyById(supplyId);
        if (supply.tonKho < totalQty) {
          throw new BadRequestException(
            `Vật tư "${supply.tenVatTu}" không đủ tồn kho. Tồn: ${supply.tonKho}, Tổng yêu cầu: ${totalQty}`
          );
        }
      }

      // Create allocations with small delay to prevent duplicate maPhieu
      const createdAllocations = [];
      for (const [, items] of groupedByClinic.entries()) {
        const firstItem = items[0];

        const allocationDto: CreateAllocationDto = {
          phongKham: firstItem.phongKham,
          hinhThucVanChuyen: firstItem.hinhThucVanChuyen as DeliveryMethod,
          danhSachVatTu: items.map(item => ({
            vatTu: item.vatTu,
            tenVatTu: item.tenVatTu,
            soLuong: item.soLuong,
          })),
          nguoiTaoPhieu,
        };

        const allocation = await this.createAllocation(allocationDto);
        createdAllocations.push(allocation);

        // Small delay to ensure unique timestamps
        await new Promise(resolve => setTimeout(resolve, 10));
      }

      return {
        success: true,
        message: `Đã tạo ${createdAllocations.length} phiếu cấp phát thành công`,
        allocations: createdAllocations,
      };
    } catch (error) {
      console.error('Excel import error:', error);

      if (error instanceof BadRequestException) {
        throw error;
      }

      // Log more details about the error
      if (error instanceof Error) {
        console.error('Error details:', error.message, error.stack);
      }

      throw new BadRequestException('Không thể đọc file Excel. Vui lòng kiểm tra định dạng file.');
    }
  }

  // ============ BÁO CÁO TỒN KHO ============

  async getAllocationDetailReport(params?: {
    phongKham?: string;
    search?: string;
    startDate?: string;
    endDate?: string;
    page?: number;
    limit?: number;
  }): Promise<{
    data: any[];
    total: number;
    page: number;
    limit: number;
    totalPages: number;
  }> {
    const { phongKham, search, startDate, endDate, page = 1, limit = 10 } = params || {};

    const filter: any = {
      trangThai: AllocationStatus.DA_GIAO,
    };

    if (phongKham) {
      filter.phongKham = phongKham; // Use string instead of ObjectId
    }

    if (startDate || endDate) {
      filter.ngayGiao = {};
      if (startDate) {
        filter.ngayGiao.$gte = new Date(startDate);
      }
      if (endDate) {
        const end = new Date(endDate);
        end.setHours(23, 59, 59, 999);
        filter.ngayGiao.$lte = end;
      }
    }

    // Search by maPhieu or supply name
    if (search) {
      filter.$or = [
        { maPhieu: { $regex: search, $options: 'i' } },
        { 'danhSachVatTu.tenVatTu': { $regex: search, $options: 'i' } },
      ];
    }

    // Fetch ALL allocations matching filter (no pagination yet)
    const allocations = await this.allocationModel
      .find(filter)
      .populate('phongKham', 'maPhongKham tenPhongKham')
      .sort({ ngayGiao: -1 })
      .exec();

    // Flatten data - one row per supply item
    const allData: any[] = [];
    const now = new Date();
    now.setHours(0, 0, 0, 0); // Reset to start of day for comparison

    for (const allocation of allocations) {
      for (const item of allocation.danhSachVatTu) {
        const soLuongDaNhan = item.soLuongDaNhan || 0;
        const soLuongTon = item.soLuong - soLuongDaNhan;
        // Cap usage percentage at 100%
        const tyLeSuDung = item.soLuong > 0 ? Math.min(Math.round((soLuongDaNhan / item.soLuong) * 100), 100) : 0;

        // Calculate expiry warning
        let hanSuDung = null;
        let canhBaoHan = 'Chưa có thông tin';

        if (item.hanSuDung) {
          hanSuDung = item.hanSuDung;
          const expiryDate = new Date(item.hanSuDung);
          expiryDate.setHours(0, 0, 0, 0);

          const daysUntilExpiry = Math.ceil((expiryDate.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));

          if (daysUntilExpiry < 0) {
            canhBaoHan = 'Đã hết hạn';
          } else if (daysUntilExpiry <= 3) {
            canhBaoHan = 'Sắp hết hạn';
          } else {
            canhBaoHan = 'Chưa đến hạn';
          }
        }

        allData.push({
          _id: `${allocation._id}_${item.vatTu}`,
          ngayCap: allocation.ngayGiao,
          maPhieu: allocation.maPhieu,
          phongKham: (allocation.phongKham as any)?.tenPhongKham || '',
          tenVatTu: item.tenVatTu,
          soLuongCap: item.soLuong,
          soLuongDaDung: soLuongDaNhan,
          tonKho: soLuongTon,
          tyLeSuDung: `${tyLeSuDung}%`,
          hanSuDung: hanSuDung,
          canhBaoHan: canhBaoHan,
        });
      }
    }

    // Apply pagination AFTER flattening
    const total = allData.length;
    const skip = (page - 1) * limit;
    const paginatedData = allData.slice(skip, skip + limit);

    return {
      data: paginatedData,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async getInventoryReport(params?: {
    phongKham?: string;
    vatTu?: string;
    startDate?: string;
    endDate?: string;
  }): Promise<any> {
    const { phongKham, vatTu, startDate, endDate } = params || {};

    // Get all delivered allocations
    const allocationFilter: any = {
      trangThai: AllocationStatus.DA_GIAO,
    };

    if (phongKham) {
      allocationFilter.phongKham = phongKham; // Use string
    }

    const allocations = await this.allocationModel
      .find(allocationFilter)
      .populate('phongKham', 'maPhongKham tenPhongKham')
      .exec();

    // Get all sample return history
    const historyFilter: any = {
      loaiThayDoi: HistoryType.NHAN_MAU_VE,
    };

    const sampleReturnHistory = await this.historyModel
      .find(historyFilter)
      .populate('phieuCapPhat')
      .exec();

    // Get all clinics for lookup
    const allClinics = await this.clinicModel.find().exec();
    const clinicMap = new Map(allClinics.map(c => [c._id.toString(), c]));
    const clinicByCodeMap = new Map(allClinics.map(c => [c.maPhongKham, c]));

    // Build report data grouped by (clinic, supply)
    const reportMap: Map<string, any> = new Map();

    // Process allocations
    for (const allocation of allocations) {
      const clinicId = (allocation.phongKham as any)._id.toString();
      const clinicName = (allocation.phongKham as any).tenPhongKham;

      for (const supplyItem of allocation.danhSachVatTu) {
        const supplyId = supplyItem.vatTu.toString();
        const key = `${clinicId}_${supplyId}`;

        if (!reportMap.has(key)) {
          reportMap.set(key, {
            clinicId,
            clinicName,
            supplyId,
            soLuongCap: 0,
            soLuongDaDung: 0,
            lastReturnDate: null,
          });
        }

        const entry = reportMap.get(key);
        entry.soLuongCap += supplyItem.soLuong;
        entry.soLuongDaDung += supplyItem.soLuongDaNhan || 0;
      }
    }

    // Process sample return history to find last return date AND handle returns without allocation
    for (const history of sampleReturnHistory) {
      let clinicId: string;
      let clinicName: string;
      let clinic: any;
      
      if (history.phieuCapPhat) {
        // Has allocation - get clinic from allocation
        const allocation = await this.allocationModel
          .findById(history.phieuCapPhat)
          .populate('phongKham')
          .exec();

        if (!allocation || !allocation.phongKham) continue;

        clinic = allocation.phongKham;
        clinicId = (clinic as any)._id.toString();
        clinicName = (clinic as any).tenPhongKham;
      } else if (history.phongKham) {
        // No allocation but has clinic in history
        if (typeof history.phongKham === 'object' && (history.phongKham as any)._id) {
          // phongKham is populated object
          clinic = history.phongKham;
          clinicId = (clinic as any)._id.toString();
          clinicName = (clinic as any).tenPhongKham || 'N/A';
        } else if (typeof history.phongKham === 'string') {
          // phongKham is string (code or ID)
          clinic = clinicByCodeMap.get(history.phongKham);
          
          if (!clinic) {
            // Try to find by ID if phongKham is actually an ID
            clinic = clinicMap.get(history.phongKham);
          }
          
          if (!clinic) continue; // Skip if clinic not found

          clinicId = clinic._id.toString();
          clinicName = clinic.tenPhongKham || 'N/A';
        } else {
          continue; // Unknown format
        }
      } else {
        // Skip if no clinic info
        continue;
      }

      // Check if vatTu exists
      if (!history.vatTu) continue;

      const supplyId = history.vatTu.toString();
      const key = `${clinicId}_${supplyId}`;

      if (!reportMap.has(key)) {
        // Create entry for returns without allocation
        reportMap.set(key, {
          clinicId,
          clinicName,
          supplyId,
          soLuongCap: 0,
          soLuongDaDung: 0,
          lastReturnDate: null,
        });
      }

      const entry = reportMap.get(key);
      
      // Update soLuongDaDung (used quantity)
      entry.soLuongDaDung += history.soLuong || 0;
      
      // Update last return date
      const returnDate = history.thoiGian;
      if (!entry.lastReturnDate || returnDate > entry.lastReturnDate) {
        entry.lastReturnDate = returnDate;
      }
    }

    // Calculate previous month usage
    const now = new Date();
    const lastMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
    const lastMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);

    const lastMonthHistory = await this.historyModel
      .find({
        loaiThayDoi: HistoryType.NHAN_MAU_VE,
        thoiGian: {
          $gte: lastMonthStart,
          $lte: lastMonthEnd,
        },
      })
      .populate('phieuCapPhat')
      .exec();

    const lastMonthUsageMap: Map<string, number> = new Map();

    for (const history of lastMonthHistory) {
      if (!history.phieuCapPhat) continue;

      const allocation = await this.allocationModel
        .findById(history.phieuCapPhat)
        .populate('phongKham')
        .exec();

      if (!allocation) continue;

      const clinicId = (allocation.phongKham as any)._id.toString();
      const supplyId = history.vatTu.toString();
      const key = `${clinicId}_${supplyId}`;

      const currentUsage = lastMonthUsageMap.get(key) || 0;
      lastMonthUsageMap.set(key, currentUsage + history.soLuong);
    }

    // Get all supplies
    const supplies = await this.supplyModel.find().exec();
    const supplyMap = new Map(supplies.map(s => [s._id.toString(), s]));

    // Build final report
    const reportData: any[] = [];

    for (const [key, entry] of reportMap.entries()) {
      const supply = supplyMap.get(entry.supplyId);
      if (!supply) continue;

      // Skip if filtering by specific supply
      if (vatTu && supply._id.toString() !== vatTu) {
        continue;
      }

      const soLuongTon = entry.soLuongCap - entry.soLuongDaDung;
      const lastMonthUsage = lastMonthUsageMap.get(key) || 0;

      // Calculate date warning
      let canhBaoNgay = 'Bình thường';
      let ngayGuiMauGanNhat = 'Chưa gửi';

      if (entry.lastReturnDate) {
        ngayGuiMauGanNhat = entry.lastReturnDate.toISOString().split('T')[0];
        const daysSinceLastReturn = Math.floor(
          (now.getTime() - entry.lastReturnDate.getTime()) / (1000 * 60 * 60 * 24)
        );
        if (daysSinceLastReturn > 7) {
          canhBaoNgay = 'Lâu chưa gửi mẫu';
        }
      }

      // Calculate stock warning for clinic
      let canhBaoTonKho = 'Đủ dùng';
      if (soLuongTon <= 0) {
        canhBaoTonKho = 'Hết';
      } else if (soLuongTon <= 5) {
        canhBaoTonKho = 'Sắp hết';
      }

      reportData.push({
        _id: key,
        phongKham: entry.clinicName,
        tenDungCu: supply.tenVatTu,
        slCap: entry.soLuongCap,
        slSuDung: entry.soLuongDaDung,
        slTon: soLuongTon,
        ngayGuiMauGanNhat,
        canhBaoNgay,
        canhBaoTonKho,
        slSuDungThangTruoc: lastMonthUsage,
      });
    }

    return reportData;
  }

  // ============ NHẬP SỐ LƯỢNG MẪU NHẬN VỀ ============

  async getSampleReturnHistory(params?: {
    phongKham?: string;
    vatTu?: string;
    startDate?: string;
    endDate?: string;
    page?: number;
    limit?: number;
  }): Promise<{
    data: any[];
    total: number;
    page: number;
    limit: number;
    totalPages: number;
  }> {
    const { phongKham, vatTu, startDate, endDate, page = 1, limit = 10 } = params || {};

    const filter: any = {
      loaiThayDoi: HistoryType.NHAN_MAU_VE,
    };

    if (vatTu) {
      filter.vatTu = new Types.ObjectId(vatTu);
    }

    if (startDate || endDate) {
      filter.thoiGian = {};
      if (startDate) {
        filter.thoiGian.$gte = new Date(startDate);
      }
      if (endDate) {
        const end = new Date(endDate);
        end.setHours(23, 59, 59, 999);
        filter.thoiGian.$lte = end;
      }
    }

    const skip = (page - 1) * limit;
    const total = await this.historyModel.countDocuments(filter).exec();

    const histories = await this.historyModel
      .find(filter)
      .populate('vatTu', 'maVatTu tenVatTu donVi')
      .populate('nguoiThucHien', 'hoTen')
      .populate({
        path: 'phieuCapPhat',
        populate: {
          path: 'phongKham',
          select: 'maPhongKham tenPhongKham',
        },
      })
      .sort({ thoiGian: -1 })
      .skip(skip)
      .limit(limit)
      .exec();

    // Map data and get clinic info from either allocation or direct clinic field
    const data = await Promise.all(
      histories.map(async (h: any) => {
        let phongKham = null;

        // Try to get clinic from allocation first
        if (h.phieuCapPhat?.phongKham) {
          phongKham = h.phieuCapPhat.phongKham;
        } 
        // If no allocation, get clinic from direct field
        else if (h.phongKham) {
          const clinic = await this.historyModel.db.collection('clinics').findOne({
            _id: new Types.ObjectId(h.phongKham),
          });
          if (clinic) {
            phongKham = {
              _id: clinic._id,
              maPhongKham: clinic.maPhongKham,
              tenPhongKham: clinic.tenPhongKham,
            };
          }
        }

        return {
          _id: h._id,
          ngayNhan: h.thoiGian,
          phongKham: phongKham,
          vatTu: h.vatTu,
          soLuong: h.soLuong,
          lyDo: h.lyDo,
          nguoiNhap: h.nguoiThucHien,
          maPhieu: h.phieuCapPhat?.maPhieu || '',
          createdAt: h.createdAt,
        };
      })
    );

    // Filter by clinic if specified
    const filteredData = phongKham 
      ? data.filter((item) => item.phongKham?._id?.toString() === phongKham)
      : data;

    return {
      data: filteredData,
      total: phongKham ? filteredData.length : total,
      page,
      limit,
      totalPages: Math.ceil((phongKham ? data.length : total) / limit),
    };
  }

  async generateSampleReturnTemplate(): Promise<any> {
    // Get all delivered allocations
    const allocations = await this.allocationModel
      .find({ trangThai: AllocationStatus.DA_GIAO })
      .populate('phongKham')
      .sort({ ngayGiao: -1 })
      .limit(100)
      .exec();

    // Create workbook
    const wb = XLSX.utils.book_new();

    // Sheet 1: Template for data entry
    const templateData = [
      ['Ngày nhận mẫu', 'Mã PK', 'Mã VT', 'Số lượng vật tư'],
      // Example rows
      ['2026-03-03', 'PK001', 'VT001', 10],
      ['2026-03-03', 'PK001', 'VT002', 5],
    ];
    const ws1 = XLSX.utils.aoa_to_sheet(templateData);
    XLSX.utils.book_append_sheet(wb, ws1, 'Nhập liệu');

    // Sheet 2: Clinic reference
    const clinics = await this.allocationModel.db.collection('clinics').find().toArray();
    const clinicData = [['Mã PK', 'Tên phòng khám']];
    clinics.forEach((clinic: any) => {
      clinicData.push([clinic.maPhongKham, clinic.tenPhongKham]);
    });
    const ws2 = XLSX.utils.aoa_to_sheet(clinicData);
    XLSX.utils.book_append_sheet(wb, ws2, 'Danh sách PK');

    // Sheet 3: Supply reference
    const supplies = await this.supplyModel.find().select('maVatTu tenVatTu').exec();
    const supplyData = [['Mã VT', 'Tên vật tư']];
    supplies.forEach((supply: any) => {
      supplyData.push([supply.maVatTu, supply.tenVatTu]);
    });
    const ws3 = XLSX.utils.aoa_to_sheet(supplyData);
    XLSX.utils.book_append_sheet(wb, ws3, 'Danh sách VT');

    // Sheet 4: Recent allocations for reference
    const allocationData = [['Mã phiếu', 'Tên PK', 'Ngày giao', 'Vật tư đã cấp']];
    for (const allocation of allocations) {
      const clinic = allocation.phongKham as any;
      const vatTuList = allocation.danhSachVatTu
        .map((item) => `${item.tenVatTu} (${item.soLuong})`)
        .join(', ');
      allocationData.push([
        allocation.maPhieu,
        clinic.tenPhongKham,
        allocation.ngayGiao ? new Date(allocation.ngayGiao).toISOString().split('T')[0] : '',
        vatTuList,
      ]);
    }
    const ws4 = XLSX.utils.aoa_to_sheet(allocationData);
    XLSX.utils.book_append_sheet(wb, ws4, 'Phiếu đã giao');

    // Sheet 5: Instructions
    const instructions = [
      ['HƯỚNG DẪN NHẬP SỐ LƯỢNG MẪU NHẬN VỀ'],
      [''],
      ['1. Ngày nhận mẫu: Nhập ngày nhận mẫu về'],
      ['   - Định dạng khuyến nghị: YYYY-MM-DD (VD: 2026-02-03 cho ngày 3 tháng 2)'],
      ['   - QUAN TRỌNG: Nếu nhập DD/MM/YYYY (VD: 3/2/2026), phải format cột thành TEXT trước'],
      ['   - Cách format: Chọn cột A → Chuột phải → Format Cells → Text'],
      [''],
      ['2. Mã PK: Nhập mã phòng khám (xem sheet "Danh sách PK")'],
      ['3. Mã VT: Nhập mã vật tư (xem sheet "Danh sách VT")'],
      ['4. Số lượng vật tư: Nhập số lượng mẫu đã nhận về từ phòng khám'],
      [''],
      ['LƯU Ý QUAN TRỌNG:'],
      ['- Mỗi dòng là một vật tư nhận về từ một phòng khám'],
      ['- Hệ thống sẽ tự động trừ số lượng này vào phiếu cấp phát tương ứng'],
      ['- Số lượng nhận về không được vượt quá số lượng đã cấp trong phiếu'],
      ['- Xem sheet "Phiếu đã giao" để biết các phiếu đã giao và số lượng đã cấp'],
      ['- Chỉ nhập cho các phiếu đã ở trạng thái "Đã giao"'],
      ['- Mã PK phải khớp chính xác với mã trong hệ thống'],
      [''],
      ['VÍ DỤ:'],
      ['Nếu đã cấp cho Phòng khám ABC: Bộ cấy tiểu 20 cái'],
      ['Và nhận về: 15 cái'],
      ['Thì còn lại: 5 cái chưa nhận về'],
    ];
    const ws5 = XLSX.utils.aoa_to_sheet(instructions);
    XLSX.utils.book_append_sheet(wb, ws5, 'Hướng dẫn');

    // Generate buffer
    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });

    return {
      buffer,
      filename: `Mau_Nhap_Mau_Nhan_Ve_${new Date().toISOString().split('T')[0]}.xlsx`,
    };
  }

  async importSampleReturnFromExcel(file: Express.Multer.File, nguoiNhap: string): Promise<any> {
    if (!file) {
      throw new BadRequestException('Không có file được tải lên');
    }

    try {
      // Read Excel file
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];

      if (!sheetName) {
        throw new BadRequestException('File Excel không có sheet nào');
      }

      const worksheet = workbook.Sheets[sheetName];
      const data: any[] = XLSX.utils.sheet_to_json(worksheet);

      if (data.length === 0) {
        throw new BadRequestException('File Excel không có dữ liệu');
      }

      const errors: string[] = [];
      const processedRecords: any[] = [];

      for (let i = 0; i < data.length; i++) {
        const row = data[i];
        const rowNum = i + 2; // Excel row number (header is row 1)

        // Skip empty rows
        if (!row['Ngày nhận mẫu'] && !row['Mã PK'] && !row['Mã VT'] && !row['Số lượng vật tư']) {
          continue;
        }

        // Validate required fields
        if (!row['Ngày nhận mẫu']) {
          errors.push(`Dòng ${rowNum}: Thiếu ngày nhận mẫu`);
          continue;
        }
        if (!row['Mã PK']) {
          errors.push(`Dòng ${rowNum}: Thiếu mã phòng khám`);
          continue;
        }
        if (!row['Mã VT']) {
          errors.push(`Dòng ${rowNum}: Thiếu Mã VT`);
          continue;
        }

        const quantity = Number(row['Số lượng vật tư']);
        if (!row['Số lượng vật tư'] || isNaN(quantity) || quantity <= 0 || !Number.isInteger(quantity)) {
          errors.push(`Dòng ${rowNum}: Số lượng vật tư không hợp lệ (phải là số nguyên dương)`);
          continue;
        }

        // Parse date - support multiple formats
        let ngayNhanMau: Date;
        const dateInput = row['Ngày nhận mẫu'];

        console.log(`Row ${rowNum} - Raw date input:`, dateInput, `Type: ${typeof dateInput}`);

        // Try parsing as Excel serial number first
        if (typeof dateInput === 'number') {
          // Excel date serial number (days since 1900-01-01, with bug for 1900 leap year)
          // Excel incorrectly treats 1900 as a leap year, so we need to account for that
          const excelEpoch = new Date(Date.UTC(1899, 11, 30)); // December 30, 1899
          ngayNhanMau = new Date(excelEpoch.getTime() + dateInput * 86400000);
          console.log(`Row ${rowNum} - Parsed from Excel serial ${dateInput}: ${ngayNhanMau.toISOString()}`);
        } else if (typeof dateInput === 'string') {
          // Try DD/MM/YYYY format first (Vietnamese format)
          const ddmmyyyyMatch = dateInput.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
          if (ddmmyyyyMatch) {
            const [, day, month, year] = ddmmyyyyMatch;
            console.log(`Row ${rowNum} - Matched DD/MM/YYYY: day=${day}, month=${month}, year=${year}`);
            // Create date in UTC to avoid timezone issues
            ngayNhanMau = new Date(Date.UTC(parseInt(year), parseInt(month) - 1, parseInt(day)));
            console.log(`Row ${rowNum} - Parsed date: ${ngayNhanMau.toISOString()}`);
          } else {
            // Try standard date parsing (YYYY-MM-DD, etc.)
            ngayNhanMau = new Date(dateInput);
            console.log(`Row ${rowNum} - Parsed using standard: ${ngayNhanMau.toISOString()}`);
          }
        } else if (dateInput instanceof Date) {
          // Already a Date object (Excel might parse it)
          ngayNhanMau = dateInput;
          console.log(`Row ${rowNum} - Already Date object: ${ngayNhanMau.toISOString()}`);
        } else {
          // Try direct conversion
          ngayNhanMau = new Date(dateInput);
          console.log(`Row ${rowNum} - Direct conversion: ${ngayNhanMau.toISOString()}`);
        }

        if (isNaN(ngayNhanMau.getTime())) {
          errors.push(`Dòng ${rowNum}: Ngày nhận mẫu không hợp lệ (định dạng: DD/MM/YYYY hoặc YYYY-MM-DD)`);
          continue;
        }

        console.log(`Row ${rowNum} - Final date stored: ${ngayNhanMau.toISOString().split('T')[0]}`);

        // Find clinic by code
        const clinic = await this.allocationModel.db.collection('clinics').findOne({
          maPhongKham: row['Mã PK'].trim()
        });
        if (!clinic) {
          errors.push(`Dòng ${rowNum}: Không tìm thấy phòng khám với mã "${row['Mã PK']}"`);
          continue;
        }

        // Find supply
        const supply = await this.supplyModel.findOne({ maVatTu: row['Mã VT'] }).exec();
        if (!supply) {
          errors.push(`Dòng ${rowNum}: Không tìm thấy vật tư với mã "${row['Mã VT']}"`);
          continue;
        }

        processedRecords.push({
          rowNum,
          ngayNhanMau,
          clinicId: clinic._id,
          clinicCode: clinic.maPhongKham,
          clinicName: clinic.tenPhongKham,
          supplyId: supply._id,
          supplyCode: supply.maVatTu,
          supplyName: supply.tenVatTu,
          soLuongNhan: quantity,
        });
      }

      if (errors.length > 0) {
        throw new BadRequestException({
          message: 'Có lỗi trong file Excel',
          errors,
        });
      }

      // Process each record - find matching allocations and update
      const updatedAllocations: any[] = [];
      const updateErrors: string[] = [];

      for (const record of processedRecords) {
        // Find ALL delivered allocations for this clinic (to determine date ranges)
        const allAllocations = await this.allocationModel
          .find({
            phongKham: record.clinicId.toString(),
            trangThai: AllocationStatus.DA_GIAO,
          })
          .sort({ ngayGiao: 1 }) // Oldest first for range calculation
          .exec();

        // Filter allocations that have this supply
        const allocationsWithSupply = allAllocations.filter(allocation => {
          return allocation.danhSachVatTu.some(item => {
            return item.vatTu.toString() === record.supplyId.toString();
          });
        });

        // If no matching allocation found, allow import without allocation (clinic using their own supplies)
        if (allocationsWithSupply.length === 0) {
          // Save history record without allocation reference but with clinic info
          await this.saveHistory({
            vatTu: record.supplyId,
            loaiThayDoi: HistoryType.NHAN_MAU_VE,
            soLuong: record.soLuongNhan,
            lyDo: `Nhận mẫu về từ ${record.clinicName} - Ngày: ${record.ngayNhanMau.toISOString().split('T')[0]} (Chưa có phiếu cấp)`,
            nguoiThucHien: nguoiNhap,
            phieuCapPhat: null, // No allocation reference
            phongKham: record.clinicId.toString(), // Store clinic ID directly
            thoiGian: record.ngayNhanMau,
          });

          updatedAllocations.push({
            ...record,
            maPhieu: null,
          });
          continue;
        }

        // Find the correct allocation based on date range logic:
        // allocation.ngayGiao <= ngayNhanMau < nextAllocation.ngayGiao
        let targetAllocation = null;
        
        // Convert sample date to comparable format (YYYY-MM-DD string)
        const sampleDateStr = record.ngayNhanMau.toISOString().split('T')[0];
        
        console.log(`\n=== ALLOCATION MATCHING for ${record.supplyName} ===`);
        console.log(`Sample date: ${sampleDateStr}`);
        console.log(`Available allocations (${allocationsWithSupply.length}):`);
        allocationsWithSupply.forEach((alloc, idx) => {
          const allocDateStr = new Date(alloc.ngayGiao).toISOString().split('T')[0];
          console.log(`  [${idx}] ${alloc.maPhieu}: ${allocDateStr}`);
        });
        
        for (let i = 0; i < allocationsWithSupply.length; i++) {
          const currentAllocation = allocationsWithSupply[i];
          const nextAllocation = allocationsWithSupply[i + 1];
          
          // Convert to YYYY-MM-DD strings for comparison
          const currentDateStr = new Date(currentAllocation.ngayGiao).toISOString().split('T')[0];
          const nextDateStr = nextAllocation ? new Date(nextAllocation.ngayGiao).toISOString().split('T')[0] : null;
          
          console.log(`\nChecking [${i}] ${currentAllocation.maPhieu}:`);
          console.log(`  Current: ${currentDateStr}, Sample: ${sampleDateStr}, Next: ${nextDateStr || 'none'}`);
          console.log(`  Sample >= Current? ${sampleDateStr >= currentDateStr}`);
          
          // Check if sample date >= current allocation date
          if (sampleDateStr >= currentDateStr) {
            // If there's a next allocation, check if sample date < next allocation date
            if (nextAllocation && nextDateStr) {
              console.log(`  Sample < Next? ${sampleDateStr < nextDateStr}`);
              if (sampleDateStr < nextDateStr) {
                console.log(`  ✓ MATCH! Using ${currentAllocation.maPhieu}`);
                targetAllocation = currentAllocation;
                break;
              }
            } else {
              // No next allocation, so this is the last one - use it
              console.log(`  ✓ MATCH! Last allocation ${currentAllocation.maPhieu}`);
              targetAllocation = currentAllocation;
              break;
            }
          }
        }
        
        if (targetAllocation) {
          console.log(`\n✓ Final: ${targetAllocation.maPhieu}\n`);
        } else {
          console.log(`\n✗ No match found\n`);
        }

        if (!targetAllocation) {
          // Save history record without allocation reference but with clinic info
          await this.saveHistory({
            vatTu: record.supplyId,
            loaiThayDoi: HistoryType.NHAN_MAU_VE,
            soLuong: record.soLuongNhan,
            lyDo: `Nhận mẫu về từ ${record.clinicName} - Ngày: ${record.ngayNhanMau.toISOString().split('T')[0]} (Chưa có phiếu cấp)`,
            nguoiThucHien: nguoiNhap,
            phieuCapPhat: null,
            phongKham: record.clinicId.toString(),
            thoiGian: record.ngayNhanMau,
          });

          updatedAllocations.push({
            ...record,
            maPhieu: null,
          });
          continue;
        }
        const supplyItem = targetAllocation.danhSachVatTu.find(
          (item) => item.vatTu.toString() === record.supplyId.toString()
        );

        if (supplyItem) {
          // Update the allocation
          const soLuongDaNhan = supplyItem.soLuongDaNhan || 0;
          supplyItem.soLuongDaNhan = soLuongDaNhan + record.soLuongNhan;

          // Mark the nested array as modified so Mongoose saves it
          targetAllocation.markModified('danhSachVatTu');
          await targetAllocation.save();

          // Save history record
          await this.saveHistory({
            vatTu: record.supplyId,
            loaiThayDoi: HistoryType.NHAN_MAU_VE,
            soLuong: record.soLuongNhan,
            lyDo: `Nhận mẫu về từ ${record.clinicName} - Ngày: ${record.ngayNhanMau.toISOString().split('T')[0]}`,
            nguoiThucHien: nguoiNhap,
            phieuCapPhat: targetAllocation._id,
            thoiGian: record.ngayNhanMau,
          });

          updatedAllocations.push({
            ...record,
            maPhieu: targetAllocation.maPhieu,
          });
        }
      }

      if (updateErrors.length > 0) {
        // Return partial success with warnings
        return {
          success: true,
          message: `Đã xử lý ${updatedAllocations.length} bản ghi, có ${updateErrors.length} cảnh báo`,
          warnings: updateErrors,
          processed: updatedAllocations.length,
          total: processedRecords.length,
        };
      }

      return {
        success: true,
        message: `Đã nhập ${updatedAllocations.length} bản ghi thành công`,
        processed: updatedAllocations.length,
        details: updatedAllocations,
      };
    } catch (error) {
      console.error('Excel import error:', error);

      if (error instanceof BadRequestException) {
        throw error;
      }

      if (error instanceof Error) {
        console.error('Error details:', error.message, error.stack);
      }

      throw new BadRequestException('Không thể đọc file Excel. Vui lòng kiểm tra định dạng file.');
    }
  }

  // ============ XÓA LỊCH SỬ NHẬP MẪU NHẬN VỀ ============

  async deleteSampleReturnHistory(id: string): Promise<any> {
    // Find the history record
    const history = await this.historyModel.findById(id).exec();

    if (!history) {
      throw new NotFoundException('Không tìm thấy bản ghi lịch sử');
    }

    // Only allow deleting NHAN_MAU_VE type
    if (history.loaiThayDoi !== HistoryType.NHAN_MAU_VE) {
      throw new BadRequestException('Chỉ có thể xóa bản ghi nhập mẫu nhận về');
    }

    // Find the allocation and revert soLuongDaNhan
    if (history.phieuCapPhat) {
      const allocation = await this.allocationModel
        .findById(history.phieuCapPhat)
        .exec();

      if (allocation) {
        // Find the supply item in allocation
        const supplyItem = allocation.danhSachVatTu.find(
          (item) => item.vatTu.toString() === history.vatTu.toString()
        );

        if (supplyItem) {
          // Revert the soLuongDaNhan
          const currentDaNhan = supplyItem.soLuongDaNhan || 0;
          const revertAmount = history.soLuong;

          supplyItem.soLuongDaNhan = Math.max(0, currentDaNhan - revertAmount);

          // Mark the nested array as modified so Mongoose saves it
          allocation.markModified('danhSachVatTu');
          await allocation.save();
        }
      }
    }

    // Delete the history record
    await this.historyModel.findByIdAndDelete(id).exec();

    return {
      success: true,
      message: 'Đã xóa bản ghi thành công',
    };
  }

  // ============ MIGRATION ============


}
