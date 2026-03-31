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

    // Trừ tồn kho (không cần cập nhật hạn sử dụng nữa)
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
        loaiVatTu: supply.loaiVatTu, // Lưu loại vật tư
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
      .populate({
        path: 'nguoiThucHien',
        select: 'hoTen',
        options: { strictPopulate: false }, // Allow null/invalid references
      })
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

  // DEPRECATED: Báo cáo theo phiếu cấp không còn dùng nữa
  // Chỉ dùng báo cáo tồn kho (getInventoryReport)
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

    // If filtering by clinic, also filter history by phongKham
    if (phongKham) {
      historyFilter.phongKham = phongKham;
    }

    const sampleReturnHistory = await this.historyModel
      .find(historyFilter)
      .populate('phieuCapPhat')
      .exec();

    // OPTIMIZATION: Load all supplies once and cache in Maps
    const allSupplies = await this.supplyModel.find().exec();
    const supplyMap = new Map(allSupplies.map(s => [s._id.toString(), s]));
    
    // Group supplies by loaiVatTu for fast lookup
    const suppliesByType = new Map<string, any[]>();
    for (const supply of allSupplies) {
      if (!suppliesByType.has(supply.loaiVatTu)) {
        suppliesByType.set(supply.loaiVatTu, []);
      }
      suppliesByType.get(supply.loaiVatTu).push(supply);
    }

    // Cache allocations by ID for fast lookup
    const allocationMap = new Map(allocations.map(a => [a._id.toString(), a]));

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

    // Process sample return history to find last return date ONLY (not for calculating usage)
    for (const history of sampleReturnHistory) {
      let clinicId: string;
      let clinicName: string;
      let clinic: any;

      if (history.phieuCapPhat) {
        // Has allocation - get from cache instead of querying
        const allocation = allocationMap.get(history.phieuCapPhat.toString());

        if (!allocation || !allocation.phongKham) continue;

        clinic = allocation.phongKham;
        clinicId = (clinic as any)._id.toString();
        clinicName = (clinic as any).tenPhongKham;

        // If filtering by clinic, skip if this allocation is not for the filtered clinic
        if (phongKham && clinicId !== phongKham) {
          continue;
        }

        // Nếu có loaiVatTu, cập nhật lastReturnDate cho TẤT CẢ vật tư thuộc loại đó
        if (history.loaiVatTu) {
          for (const supplyItem of allocation.danhSachVatTu) {
            // Use cached supply instead of querying
            const supply = supplyMap.get(supplyItem.vatTu.toString());
            if (!supply || supply.loaiVatTu !== history.loaiVatTu) continue;

            const supplyId = supply._id.toString();
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
            
            // ONLY update last return date, NOT soLuongDaDung
            // (soLuongDaDung is already calculated from allocation.danhSachVatTu[].soLuongDaNhan)
            const returnDate = history.thoiGian;
            if (!entry.lastReturnDate || returnDate > entry.lastReturnDate) {
              entry.lastReturnDate = returnDate;
            }
          }
          continue; // Đã xử lý xong, bỏ qua phần còn lại
        }

        // For history without loaiVatTu, just update lastReturnDate for the specific supply
        if (history.vatTu) {
          const supplyId = history.vatTu.toString();
          const key = `${clinicId}_${supplyId}`;

          if (reportMap.has(key)) {
            const entry = reportMap.get(key);
            const returnDate = history.thoiGian;
            if (!entry.lastReturnDate || returnDate > entry.lastReturnDate) {
              entry.lastReturnDate = returnDate;
            }
          }
        }
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

        // Nếu có loaiVatTu, cập nhật cho TẤT CẢ vật tư thuộc loại đó
        if (history.loaiVatTu) {
          // Use cached supplies by type instead of querying
          const suppliesOfType = suppliesByType.get(history.loaiVatTu) || [];
          
          for (const supply of suppliesOfType) {
            const supplyId = supply._id.toString();
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

            // For returns without allocation, we DO count the usage
            entry.soLuongDaDung += history.soLuong || 0;

            // Update last return date
            const returnDate = history.thoiGian;
            if (!entry.lastReturnDate || returnDate > entry.lastReturnDate) {
              entry.lastReturnDate = returnDate;
            }
          }
          continue; // Đã xử lý xong
        }

        // Nếu KHÔNG có loaiVatTu, xử lý theo vật tư cụ thể (cách cũ)
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

        // For returns without allocation, we DO count the usage
        entry.soLuongDaDung += history.soLuong || 0;

        // Update last return date
        const returnDate = history.thoiGian;
        if (!entry.lastReturnDate || returnDate > entry.lastReturnDate) {
          entry.lastReturnDate = returnDate;
        }
      }
    }

    // Calculate previous month usage
    const now = new Date();
    const lastMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1);
    const lastMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0, 23, 59, 59, 999);

    const lastMonthHistoryFilter: any = {
      loaiThayDoi: HistoryType.NHAN_MAU_VE,
      thoiGian: {
        $gte: lastMonthStart,
        $lte: lastMonthEnd,
      },
    };

    // If filtering by clinic, also filter last month history
    if (phongKham) {
      lastMonthHistoryFilter.phongKham = phongKham;
    }

    const lastMonthHistory = await this.historyModel
      .find(lastMonthHistoryFilter)
      .populate('phieuCapPhat')
      .exec();

    const lastMonthUsageMap: Map<string, number> = new Map();

    for (const history of lastMonthHistory) {
      if (!history.phieuCapPhat) {
        // History without allocation - use phongKham directly
        if (!history.phongKham) continue;

        let clinicId: string;
        if (typeof history.phongKham === 'object' && (history.phongKham as any)._id) {
          clinicId = (history.phongKham as any)._id.toString();
        } else if (typeof history.phongKham === 'string') {
          clinicId = history.phongKham;
        } else {
          continue;
        }

        // If filtering by clinic, skip if not matching
        if (phongKham && clinicId !== phongKham) {
          continue;
        }

        // Process history with loaiVatTu - use cached supplies
        if (history.loaiVatTu) {
          const suppliesOfType = suppliesByType.get(history.loaiVatTu) || [];
          for (const supply of suppliesOfType) {
            const supplyId = supply._id.toString();
            const key = `${clinicId}_${supplyId}`;
            const currentUsage = lastMonthUsageMap.get(key) || 0;
            lastMonthUsageMap.set(key, currentUsage + history.soLuong);
          }
        } else if (history.vatTu) {
          const supplyId = history.vatTu.toString();
          const key = `${clinicId}_${supplyId}`;
          const currentUsage = lastMonthUsageMap.get(key) || 0;
          lastMonthUsageMap.set(key, currentUsage + history.soLuong);
        }
        continue;
      }

      // Use cached allocation instead of querying
      const allocation = allocationMap.get(history.phieuCapPhat.toString());

      if (!allocation) continue;

      const clinicId = (allocation.phongKham as any)._id.toString();

      // If filtering by clinic, skip if this allocation is not for the filtered clinic
      if (phongKham && clinicId !== phongKham) {
        continue;
      }

      // Nếu có loaiVatTu, tính cho TẤT CẢ vật tư thuộc loại đó
      if (history.loaiVatTu) {
        for (const supplyItem of allocation.danhSachVatTu) {
          // Use cached supply instead of querying
          const supply = supplyMap.get(supplyItem.vatTu.toString());
          if (!supply || supply.loaiVatTu !== history.loaiVatTu) continue;

          const supplyId = supply._id.toString();
          const key = `${clinicId}_${supplyId}`;
          const currentUsage = lastMonthUsageMap.get(key) || 0;
          lastMonthUsageMap.set(key, currentUsage + history.soLuong);
        }
      } else {
        // Cách cũ: chỉ tính cho 1 vật tư
        const supplyId = history.vatTu.toString();
        const key = `${clinicId}_${supplyId}`;
        const currentUsage = lastMonthUsageMap.get(key) || 0;
        lastMonthUsageMap.set(key, currentUsage + history.soLuong);
      }
    }

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
      if (soLuongTon < 0) {
        canhBaoTonKho = 'Âm';
      } else if (soLuongTon === 0) {
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
      .populate({
        path: 'nguoiThucHien',
        select: 'hoTen',
        options: { strictPopulate: false }, // Allow null/invalid references
      })
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
          thoiGian: h.thoiGian, // Thêm thoiGian để frontend dùng
          phongKham: phongKham,
          vatTu: h.vatTu,
          loaiVatTu: h.loaiVatTu, // Thêm loaiVatTu
          soLuong: h.soLuong,
          lyDo: h.lyDo,
          nguoiNhap: h.nguoiThucHien,
          maPhieu: h.phieuCapPhat?.maPhieu || '',
          createdAt: h.createdAt,
          receivedAt: h.receivedAt, // Thêm receivedAt
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
      ['Ngày nhận mẫu', 'Mã PK', 'Loại vật tư', 'Số lượng mẫu'],
      // Example rows
      ['2026-03-03', 'PK001', 'NIPT', 1],
      ['2026-03-03', 'PK001', 'ADN', 2],
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

    // Sheet 3: Supply type reference
    const supplyTypeData = [
      ['Loại vật tư', 'Mô tả'],
      ['NIPT', 'Xét nghiệm NIPT (bao gồm ống nghiệm + kim tiêm)'],
      ['ADN', 'Xét nghiệm ADN'],
      ['CELL', 'Xét nghiệm tế bào'],
      ['HPV', 'Xét nghiệm HPV'],
      ['KHAC', 'Loại khác'],
    ];
    const ws3 = XLSX.utils.aoa_to_sheet(supplyTypeData);
    XLSX.utils.book_append_sheet(wb, ws3, 'Loại vật tư');

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
      ['2. Mã PK: Nhập mã phòng khám hoặc tên phòng khám (xem sheet "Danh sách PK")'],
      ['3. Loại vật tư: Nhập loại vật tư (NIPT, ADN, CELL, HPV, KHAC - xem sheet "Loại vật tư")'],
      ['4. Số lượng mẫu: Nhập số lượng MẪU đã nhận về (VD: 1 mẫu NIPT = 1 ống nghiệm + 1 kim tiêm)'],
      [''],
      ['LƯU Ý QUAN TRỌNG:'],
      ['- Mỗi dòng là một LOẠI MẪU nhận về từ một phòng khám'],
      ['- VD: Nhận 1 mẫu NIPT = hệ thống tự động trừ 1 ống nghiệm + 1 kim tiêm'],
      ['- Hệ thống sẽ tự động trừ TẤT CẢ vật tư thuộc loại đó trong phiếu cấp'],
      ['- Xem sheet "Phiếu đã giao" để biết các phiếu đã giao'],
      ['- Chỉ nhập cho các phiếu đã ở trạng thái "Đã giao"'],
      ['- Mã PK có thể là mã hoặc tên phòng khám'],
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
        if (!row['Ngày nhận mẫu'] && !row['Mã PK'] && !row['Loại vật tư'] && !row['Số lượng mẫu']) {
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
        if (!row['Loại vật tư']) {
          errors.push(`Dòng ${rowNum}: Thiếu loại vật tư`);
          continue;
        }

        const quantity = Number(row['Số lượng mẫu']);
        if (!row['Số lượng mẫu'] || isNaN(quantity) || quantity <= 0 || !Number.isInteger(quantity)) {
          errors.push(`Dòng ${rowNum}: Số lượng mẫu không hợp lệ (phải là số nguyên dương)`);
          continue;
        }

        // Validate supply type
        const validTypes = ['NIPT', 'ADN', 'CELL', 'HPV', 'KHAC'];
        const loaiVatTu = row['Loại vật tư'].trim().toUpperCase();
        if (!validTypes.includes(loaiVatTu)) {
          errors.push(`Dòng ${rowNum}: Loại vật tư không hợp lệ (phải là: ${validTypes.join(', ')})`);
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

        // Find clinic by code or name
        const clinicInput = row['Mã PK'].trim();
        const clinic = await this.allocationModel.db.collection('clinics').findOne({
          $or: [
            { maPhongKham: clinicInput },
            { tenPhongKham: clinicInput }
          ]
        });
        if (!clinic) {
          errors.push(`Dòng ${rowNum}: Không tìm thấy phòng khám với mã hoặc tên "${clinicInput}"`);
          continue;
        }

        processedRecords.push({
          rowNum,
          ngayNhanMau,
          clinicId: clinic._id,
          clinicCode: clinic.maPhongKham,
          clinicName: clinic.tenPhongKham,
          loaiVatTu: loaiVatTu,
          soLuongMau: quantity,
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
        // Convert clinicId to ObjectId for proper comparison
        const clinicObjectId = new Types.ObjectId(record.clinicId);
        
        const allAllocations = await this.allocationModel
          .find({
            phongKham: clinicObjectId,
            trangThai: AllocationStatus.DA_GIAO,
          })
          .sort({ ngayGiao: 1 }) // Oldest first for range calculation
          .exec();

        console.log(`\n=== FINDING ALLOCATIONS for clinic ${record.clinicName} ===`);
        console.log(`Clinic ID: ${record.clinicId}`);
        console.log(`Supply Type: ${record.loaiVatTu}`);
        console.log(`Found ${allAllocations.length} delivered allocations`);

        // Filter allocations that have supplies of this type
        const allocationsWithSupplyType = [];
        for (const allocation of allAllocations) {
          const hasSupplyType = await Promise.all(
            allocation.danhSachVatTu.map(async (item) => {
              const supply = await this.supplyModel.findById(item.vatTu).exec();
              return supply && supply.loaiVatTu === record.loaiVatTu;
            })
          );
          if (hasSupplyType.some(has => has)) {
            allocationsWithSupplyType.push(allocation);
          }
        }

        console.log(`Found ${allocationsWithSupplyType.length} allocations with supply type ${record.loaiVatTu}`);

        // If no matching allocation found, allow import without allocation (clinic using their own supplies)
        if (allocationsWithSupplyType.length === 0) {
          // Get first supply of this type to save in history
          const firstSupply = await this.supplyModel.findOne({ loaiVatTu: record.loaiVatTu }).exec();
          
          // Save history record without allocation reference but with clinic info
          await this.saveHistory({
            vatTu: firstSupply?._id,
            loaiVatTu: record.loaiVatTu,
            loaiThayDoi: HistoryType.NHAN_MAU_VE,
            soLuong: record.soLuongMau,
            lyDo: `Nhận ${record.soLuongMau} mẫu ${record.loaiVatTu} về từ ${record.clinicName} - Ngày: ${record.ngayNhanMau.toISOString().split('T')[0]} (Chưa có phiếu cấp)`,
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

        // FIFO Logic: Find first allocation that still has remaining stock (not fully returned)
        let targetAllocation = null;

        console.log(`\n=== FIFO ALLOCATION MATCHING for ${record.loaiVatTu} ===`);
        console.log(`Available allocations (${allocationsWithSupplyType.length}):`);

        for (const allocation of allocationsWithSupplyType) {
          // Check if this allocation still has remaining stock for this supply type
          let hasRemaining = false;
          
          for (const item of allocation.danhSachVatTu) {
            const supply = await this.supplyModel.findById(item.vatTu).exec();
            if (supply && supply.loaiVatTu === record.loaiVatTu) {
              const soLuongDaNhan = item.soLuongDaNhan || 0;
              const remaining = item.soLuong - soLuongDaNhan;
              console.log(`  ${allocation.maPhieu} - ${supply.tenVatTu}: cấp=${item.soLuong}, đã nhận=${soLuongDaNhan}, còn=${remaining}`);
              if (remaining > 0) {
                hasRemaining = true;
              }
            }
          }

          if (hasRemaining) {
            targetAllocation = allocation;
            console.log(`  ✓ MATCH! Using ${allocation.maPhieu} (còn tồn)`);
            break;
          } else {
            console.log(`  ✗ Skip ${allocation.maPhieu} (đã trừ hết)`);
          }
        }

        if (!targetAllocation) {
          console.log(`  ✗ No allocation with remaining stock found`);
          // Get first supply of this type to save in history
          const firstSupply = await this.supplyModel.findOne({ loaiVatTu: record.loaiVatTu }).exec();
          
          // Save history record without allocation reference but with clinic info
          await this.saveHistory({
            vatTu: firstSupply?._id,
            loaiVatTu: record.loaiVatTu,
            loaiThayDoi: HistoryType.NHAN_MAU_VE,
            soLuong: record.soLuongMau,
            lyDo: `Nhận ${record.soLuongMau} mẫu ${record.loaiVatTu} về từ ${record.clinicName} - Ngày: ${record.ngayNhanMau.toISOString().split('T')[0]} (Đã trừ hết phiếu cấp)`,
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

        // Update ALL supplies of the specified type in this allocation
        let updatedCount = 0;
        const firstSupplyId = targetAllocation.danhSachVatTu[0]?.vatTu; // For history reference
        
        for (const item of targetAllocation.danhSachVatTu) {
          const itemSupply = await this.supplyModel.findById(item.vatTu).exec();
          if (itemSupply && itemSupply.loaiVatTu === record.loaiVatTu) {
            const soLuongDaNhan = item.soLuongDaNhan || 0;
            item.soLuongDaNhan = soLuongDaNhan + record.soLuongMau;
            updatedCount++;
            console.log(`  Updated ${itemSupply.tenVatTu}: ${soLuongDaNhan} -> ${item.soLuongDaNhan}`);
          }
        }

        if (updatedCount > 0) {
          // Mark the nested array as modified so Mongoose saves it
          targetAllocation.markModified('danhSachVatTu');
          await targetAllocation.save();

          // Save history record with loaiVatTu
          await this.saveHistory({
            vatTu: firstSupplyId,
            loaiVatTu: record.loaiVatTu,
            loaiThayDoi: HistoryType.NHAN_MAU_VE,
            soLuong: record.soLuongMau,
            lyDo: `Nhận ${record.soLuongMau} mẫu ${record.loaiVatTu} về từ ${record.clinicName} - Ngày: ${record.ngayNhanMau.toISOString().split('T')[0]}`,
            nguoiThucHien: nguoiNhap,
            phieuCapPhat: targetAllocation._id,
            phongKham: record.clinicId.toString(),
            thoiGian: record.ngayNhanMau,
          });

          updatedAllocations.push({
            ...record,
            maPhieu: targetAllocation.maPhieu,
            updatedSuppliesCount: updatedCount,
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

  // ============ TỰ ĐỘNG TRỪ TỒN KHO THEO SERVICE TYPE ============

  async autoDeductStockByServiceType(data: {
    serviceType: string;
    caseCode: string;
    serviceName: string;
    nguoiThucHien: string;
    source?: string;
  }): Promise<any> {
    const { serviceType, caseCode, serviceName, nguoiThucHien, source } = data;

    // Tìm tất cả vật tư thuộc loại serviceType
    const supplies = await this.supplyModel
      .find({ loaiVatTu: serviceType })
      .sort({ createdAt: 1 }) // Ưu tiên vật tư cũ nhất (FIFO)
      .exec();

    if (supplies.length === 0) {
      throw new BadRequestException(`Không tìm thấy vật tư nào thuộc loại ${serviceType}`);
    }

    const deductedSupplies: any[] = [];

    // Trừ mỗi vật tư 1 đơn vị
    for (const supply of supplies) {
      // Cho phép tồn âm
      supply.tonKho -= 1;
      await supply.save();

      // Lưu lịch sử
      await this.saveHistory({
        vatTu: supply._id,
        loaiThayDoi: HistoryType.XUAT_CAP,
        soLuong: -1,
        lyDo: `Tự động trừ kho cho mẫu ${caseCode} - Dịch vụ: ${serviceName} (${serviceType})${source ? ` - Phòng khám: ${source}` : ''}`,
        nguoiThucHien,
        thoiGian: new Date(),
      });

      // Cập nhật trạng thái
      await this.updateSupplyStatus(supply._id.toString());

      deductedSupplies.push({
        maVatTu: supply.maVatTu,
        tenVatTu: supply.tenVatTu,
        tonKhoMoi: supply.tonKho,
      });
    }

    return {
      success: true,
      message: `Đã trừ tồn kho cho ${supplies.length} vật tư thuộc loại ${serviceType}`,
      caseCode,
      serviceType,
      serviceName,
      source,
      deductedSupplies,
    };
  }

  // Lấy danh sách ca từ API bên thứ 3
  async getExternalCases(params?: {
    page?: number;
    limit?: number;
  }): Promise<any> {
    const { page = 1, limit = 10 } = params || {};

    try {
      const axios = require('axios');

      console.log('Fetching external cases from API...');
      console.log('URL: https://api.gennovax.vn/api/inventory/cases');
      console.log('Params:', { page, limit });

      const response = await axios.get('https://api.gennovax.vn/api/inventory/cases', {
        params: { page, limit },
        timeout: 10000, // 10 seconds timeout
      });

      console.log('Response status:', response.status);
      console.log('Response data:', JSON.stringify(response.data).substring(0, 200));

      return {
        success: true,
        data: response.data,
      };
    } catch (error) {
      console.error('Error fetching external cases:', error);

      if (error.response) {
        // Server responded with error
        console.error('Response status:', error.response.status);
        console.error('Response data:', error.response.data);
        throw new BadRequestException(`API trả về lỗi: ${error.response.status} - ${JSON.stringify(error.response.data)}`);
      } else if (error.request) {
        // Request made but no response
        console.error('No response received:', error.request);
        throw new BadRequestException('Không nhận được phản hồi từ API bên thứ 3. Vui lòng kiểm tra kết nối mạng.');
      } else {
        // Error in request setup
        console.error('Error message:', error.message);
        throw new BadRequestException(`Lỗi khi gọi API: ${error.message}`);
      }
    }
  }

  // Đồng bộ ca từ API bên thứ 3 và tự động trừ tồn kho
  async syncExternalCasesAndDeductStock(params: {
    nguoiThucHien: string;
    startDate?: string;
    endDate?: string;
  }): Promise<any> {
    const { nguoiThucHien, startDate, endDate } = params;

    try {
      const axios = require('axios');

      console.log('Syncing external cases...');
      console.log('Params:', { nguoiThucHien, startDate, endDate });

      // Lấy tất cả ca từ API
      const response = await axios.get('https://api.gennovax.vn/api/inventory/cases', {
        params: {
          page: 1,
          limit: 1000, // Lấy nhiều để xử lý
        },
        timeout: 30000, // 30 seconds timeout
      });

      console.log('API Response status:', response.status);

      if (!response.data) {
        throw new BadRequestException('API không trả về dữ liệu');
      }

      // Xử lý cả 2 format: { data: [...] } hoặc trực tiếp [...]
      let cases = [];
      if (Array.isArray(response.data)) {
        cases = response.data;
      } else if (response.data.data && Array.isArray(response.data.data)) {
        cases = response.data.data;
      } else {
        throw new BadRequestException('Định dạng dữ liệu từ API không hợp lệ');
      }

      console.log(`Found ${cases.length} cases from API`);

      const processedCases: any[] = [];
      const errors: string[] = [];
      const skippedCases: string[] = [];

      // Lọc theo ngày nếu có
      let filteredCases = cases;
      if (startDate || endDate) {
        filteredCases = cases.filter((c: any) => {
          if (!c.createdAt) return false;
          const caseDate = new Date(c.createdAt);
          if (startDate && caseDate < new Date(startDate)) return false;
          if (endDate && caseDate > new Date(endDate)) return false;
          return true;
        });
      }

      console.log(`Processing ${filteredCases.length} cases after date filter`);

      // Xử lý từng ca
      for (const caseData of filteredCases) {
        try {
          const { serviceType, caseCode, serviceName } = caseData;

          if (!serviceType || !caseCode) {
            errors.push(`Ca ${caseCode || 'N/A'}: Thiếu thông tin serviceType hoặc caseCode`);
            continue;
          }

          // Kiểm tra xem đã xử lý ca này chưa (dựa vào caseCode)
          const existingHistory = await this.historyModel
            .findOne({
              caseCode: caseCode,
              loaiThayDoi: HistoryType.NHAN_MAU_VE,
            })
            .exec();

          if (existingHistory) {
            // Đã xử lý rồi, bỏ qua
            skippedCases.push(`${caseCode} (đã xử lý trước đó)`);
            continue;
          }

          // Tìm tất cả vật tư thuộc loại serviceType
          const supplies = await this.supplyModel
            .find({ loaiVatTu: serviceType })
            .sort({ createdAt: 1 })
            .exec();

          console.log(`Case ${caseCode}: serviceType=${serviceType}, found ${supplies.length} supplies:`, supplies.map(s => ({ id: s._id, ten: s.tenVatTu, loai: s.loaiVatTu })));

          if (supplies.length === 0) {
            errors.push(`Ca ${caseCode}: Không tìm thấy vật tư nào thuộc loại ${serviceType}`);
            continue;
          }

          // Tìm hoặc tạo phòng khám từ source
          let clinicId = null;
          let clinic = null;
          if (caseData.source) {
            clinic = await this.clinicModel.findOne({ tenPhongKham: caseData.source }).exec();
            
            if (!clinic) {
              // Tự động tạo phòng khám nếu chưa tồn tại
              const lastClinic = await this.clinicModel
                .findOne({}, { maPhongKham: 1 })
                .sort({ maPhongKham: -1 })
                .exec();

              let nextNumber = 1;
              if (lastClinic && lastClinic.maPhongKham) {
                const match = lastClinic.maPhongKham.match(/PK(\d+)/);
                if (match) {
                  nextNumber = parseInt(match[1], 10) + 1;
                }
              }

              const newClinicCode = `PK${String(nextNumber).padStart(3, '0')}`;

              clinic = await this.clinicModel.create({
                maPhongKham: newClinicCode,
                tenPhongKham: caseData.source,
                dangHoatDong: true,
              });
            }
            
            clinicId = clinic._id;
          }

          // Tìm phiếu cấp phù hợp của phòng khám này (nếu có) theo FIFO
          let targetAllocation = null;
          if (clinicId) {
            // Lấy tất cả phiếu cấp đã giao của phòng khám, có chứa vật tư thuộc loại serviceType
            const allAllocations = await this.allocationModel
              .find({
                phongKham: clinicId,
                trangThai: AllocationStatus.DA_GIAO,
              })
              .sort({ ngayGiao: 1 }) // Oldest first (FIFO)
              .exec();

            // Filter allocations that have supplies of this type
            const allocationsWithType = [];
            for (const allocation of allAllocations) {
              const hasType = await Promise.all(
                allocation.danhSachVatTu.map(async (item) => {
                  const supply = await this.supplyModel.findById(item.vatTu).exec();
                  return supply && supply.loaiVatTu === serviceType;
                })
              );
              if (hasType.some(has => has)) {
                allocationsWithType.push(allocation);
              }
            }

            // FIFO: Find first allocation that still has remaining stock
            for (const allocation of allocationsWithType) {
              let hasRemaining = false;
              
              for (const item of allocation.danhSachVatTu) {
                const supply = await this.supplyModel.findById(item.vatTu).exec();
                if (supply && supply.loaiVatTu === serviceType) {
                  const soLuongDaNhan = item.soLuongDaNhan || 0;
                  const remaining = item.soLuong - soLuongDaNhan;
                  if (remaining > 0) {
                    hasRemaining = true;
                    break;
                  }
                }
              }

              if (hasRemaining) {
                targetAllocation = allocation;
                break;
              }
            }
          }

          const deductedSupplies: any[] = [];

          // Lưu 1 bản ghi NHAN_MAU_VE cho loại vật tư này (serviceType)
          // Dùng vật tư đầu tiên làm đại diện cho loại
          if (supplies.length > 0) {
            const firstSupply = supplies[0];
            
            await this.saveHistory({
              vatTu: firstSupply._id,
              loaiVatTu: serviceType, // Lưu loại vật tư để dễ query
              loaiThayDoi: HistoryType.NHAN_MAU_VE,
              soLuong: 1, // Nhận về 1 ca
              lyDo: `Nhận mẫu ${caseCode} - Dịch vụ: ${serviceName} (${serviceType})${caseData.source ? ` - ${caseData.source}` : ''}`,
              nguoiThucHien,
              phieuCapPhat: targetAllocation ? targetAllocation._id : null,
              phongKham: clinicId ? clinicId.toString() : null,
              thoiGian: caseData.receivedAt ? new Date(caseData.receivedAt) : new Date(),
              caseCode: caseCode,
              receivedAt: caseData.receivedAt ? new Date(caseData.receivedAt) : null,
            });

            // Nếu có phiếu cấp, cập nhật soLuongDaNhan cho TẤT CẢ vật tư thuộc loại serviceType
            if (targetAllocation) {
              console.log(`Allocation ${targetAllocation.maPhieu} has supplies:`, targetAllocation.danhSachVatTu.map(item => ({ vatTu: item.vatTu, ten: item.tenVatTu, soLuong: item.soLuong, daNhan: item.soLuongDaNhan })));
              
              let updatedCount = 0;
              for (const supply of supplies) {
                const supplyItem = targetAllocation.danhSachVatTu.find(
                  (item) => item.vatTu.toString() === supply._id.toString()
                );

                if (supplyItem) {
                  const oldValue = supplyItem.soLuongDaNhan || 0;
                  supplyItem.soLuongDaNhan = oldValue + 1;
                  console.log(`  - Updated ${supply.tenVatTu}: ${oldValue} -> ${supplyItem.soLuongDaNhan}`);
                  updatedCount++;
                } else {
                  console.log(`  - Supply ${supply.tenVatTu} (${supply._id}) NOT FOUND in allocation`);
                }
              }
              
              if (updatedCount > 0) {
                targetAllocation.markModified('danhSachVatTu');
                await targetAllocation.save();
                console.log(`✓ Updated ${updatedCount} supplies in allocation ${targetAllocation.maPhieu} for case ${caseCode}`);
              } else {
                console.log(`✗ No matching supplies found in allocation ${targetAllocation.maPhieu} for serviceType ${serviceType}`);
              }
            } else {
              console.log(`No allocation found for clinic ${clinicId}`);
            }

            // Thêm tất cả vật tư vào danh sách để hiển thị
            for (const supply of supplies) {
              deductedSupplies.push({
                maVatTu: supply.maVatTu,
                tenVatTu: supply.tenVatTu,
                tonKho: supply.tonKho,
              });
            }
          }

          processedCases.push({
            caseCode,
            serviceType,
            serviceName,
            source: caseData.source,
            receivedAt: caseData.receivedAt,
            deductedSupplies,
            suppliesCount: supplies.length, // Số lượng vật tư thuộc loại này
            maPhieu: targetAllocation?.maPhieu || null,
          });
        } catch (error) {
          errors.push(`Ca ${caseData.caseCode}: ${error.message}`);
        }
      }

      return {
        success: true,
        message: `Đã xử lý ${processedCases.length} ca thành công`,
        totalCases: filteredCases.length,
        processedCases: processedCases.length,
        skippedCases: skippedCases.length,
        errorCount: errors.length,
        errors: errors.length > 0 ? errors.slice(0, 10) : undefined, // Chỉ hiển thị 10 lỗi đầu
        skipped: skippedCases.length > 0 ? skippedCases.slice(0, 10) : undefined,
        details: processedCases.slice(0, 20), // Chỉ hiển thị 20 ca đầu
      };
    } catch (error) {
      console.error('Error syncing external cases:', error);

      if (error instanceof BadRequestException) {
        throw error;
      }

      if (error.response) {
        throw new BadRequestException(`API trả về lỗi: ${error.response.status} - ${JSON.stringify(error.response.data)}`);
      } else if (error.request) {
        throw new BadRequestException('Không nhận được phản hồi từ API bên thứ 3. Vui lòng kiểm tra kết nối mạng.');
      } else {
        throw new BadRequestException(`Lỗi khi đồng bộ: ${error.message}`);
      }
    }
  }

  // ============ ĐỒNG BỘ PHÒNG KHÁM TỪ API BÊN THỨ 3 ============

  async getExternalSources(): Promise<any> {
    try {
      const axios = require('axios');

      console.log('Fetching external sources (clinics) from API...');
      console.log('URL: https://api.gennovax.vn/api/inventory/sources');

      const response = await axios.get('https://api.gennovax.vn/api/inventory/sources', {
        timeout: 10000,
      });

      console.log('Response status:', response.status);

      return {
        success: true,
        data: response.data,
      };
    } catch (error) {
      console.error('Error fetching external sources:', error);

      if (error.response) {
        throw new BadRequestException(`API trả về lỗi: ${error.response.status}`);
      } else if (error.request) {
        throw new BadRequestException('Không nhận được phản hồi từ API');
      } else {
        throw new BadRequestException(`Lỗi khi gọi API: ${error.message}`);
      }
    }
  }

  async syncExternalSources(): Promise<any> {
    try {
      const axios = require('axios');

      const response = await axios.get('https://api.gennovax.vn/api/inventory/sources', {
        timeout: 10000,
      });

      if (!response.data || !response.data.data || !Array.isArray(response.data.data)) {
        throw new BadRequestException('Dữ liệu từ API không hợp lệ');
      }

      const sources = response.data.data;
      const createdClinics: any[] = [];
      const updatedClinics: any[] = [];
      const errors: string[] = [];

      for (const sourceName of sources) {
        try {
          // Tìm phòng khám theo tên
          let clinic = await this.clinicModel.findOne({
            tenPhongKham: sourceName
          }).exec();

          if (!clinic) {
            // Tạo mã phòng khám tự động
            const lastClinic = await this.clinicModel
              .findOne({ maPhongKham: /^PK\d+$/ })
              .sort({ maPhongKham: -1 })
              .exec();

            let nextNumber = 1;
            if (lastClinic && lastClinic.maPhongKham) {
              const lastNumber = parseInt(lastClinic.maPhongKham.replace('PK', ''));
              nextNumber = lastNumber + 1;
            }

            const maPhongKham = `PK${String(nextNumber).padStart(3, '0')}`;

            // Tạo phòng khám mới
            clinic = new this.clinicModel({
              maPhongKham,
              tenPhongKham: sourceName,
              diaChi: '',
              soDienThoai: '',
            });

            await clinic.save();
            createdClinics.push({
              maPhongKham,
              tenPhongKham: sourceName,
            });
          } else {
            updatedClinics.push({
              maPhongKham: clinic.maPhongKham,
              tenPhongKham: clinic.tenPhongKham,
            });
          }
        } catch (error) {
          errors.push(`Phòng khám "${sourceName}": ${error.message}`);
        }
      }

      return {
        success: true,
        message: `Đã đồng bộ ${sources.length} phòng khám`,
        totalSources: sources.length,
        created: createdClinics.length,
        existing: updatedClinics.length,
        errors: errors.length > 0 ? errors : undefined,
        createdClinics,
      };
    } catch (error) {
      console.error('Error syncing external sources:', error);

      if (error instanceof BadRequestException) {
        throw error;
      }

      throw new BadRequestException('Không thể đồng bộ danh sách phòng khám');
    }
  }

  // Dọn dẹp các bản ghi lịch sử có nguoiThucHien không hợp lệ
  async cleanupInvalidHistory(): Promise<any> {
    try {
      // Tìm tất cả bản ghi có nguoiThucHien không phải là ObjectId hợp lệ hoặc là null
      const invalidRecords = await this.historyModel.find({
        $or: [
          { nguoiThucHien: 'system' }, // String "system"
          { nguoiThucHien: { $type: 'string' } }, // Bất kỳ string nào
        ]
      }).exec();

      console.log(`Found ${invalidRecords.length} invalid history records`);

      // Cập nhật tất cả bản ghi không hợp lệ thành null
      const result = await this.historyModel.updateMany(
        {
          $or: [
            { nguoiThucHien: 'system' },
            { nguoiThucHien: { $type: 'string' } },
          ]
        },
        { $set: { nguoiThucHien: null } }
      ).exec();

      return {
        success: true,
        message: `Đã dọn dẹp ${result.modifiedCount} bản ghi lịch sử`,
        modifiedCount: result.modifiedCount,
        foundCount: invalidRecords.length,
      };
    } catch (error) {
      console.error('Error cleaning up invalid history:', error);
      throw new BadRequestException(`Lỗi khi dọn dẹp: ${error.message}`);
    }
  }

  // Debug: Kiểm tra vật tư theo loại
  async debugCheckSuppliesByType(serviceType: string): Promise<any> {
    try {
      // Tìm tất cả vật tư thuộc loại này
      const supplies = await this.supplyModel
        .find({ loaiVatTu: serviceType })
        .exec();

      // Tìm tất cả phiếu cấp đã giao
      const allocations = await this.allocationModel
        .find({ trangThai: AllocationStatus.DA_GIAO })
        .populate('phongKham', 'tenPhongKham')
        .exec();

      // Kiểm tra xem vật tư nào có trong phiếu cấp
      const allocationDetails = allocations.map(allocation => {
        const matchingSupplies = allocation.danhSachVatTu.filter(item => 
          supplies.some(s => s._id.toString() === item.vatTu.toString())
        );

        return {
          maPhieu: allocation.maPhieu,
          phongKham: (allocation.phongKham as any)?.tenPhongKham,
          matchingSupplies: matchingSupplies.map(item => ({
            vatTu: item.vatTu,
            tenVatTu: item.tenVatTu,
            soLuong: item.soLuong,
            soLuongDaNhan: item.soLuongDaNhan || 0,
          })),
        };
      }).filter(a => a.matchingSupplies.length > 0);

      return {
        serviceType,
        suppliesFound: supplies.map(s => ({
          _id: s._id,
          maVatTu: s.maVatTu,
          tenVatTu: s.tenVatTu,
          loaiVatTu: s.loaiVatTu,
          tonKho: s.tonKho,
        })),
        totalSupplies: supplies.length,
        allocationsWithTheseSupplies: allocationDetails,
        totalAllocations: allocationDetails.length,
      };
    } catch (error) {
      throw new BadRequestException(`Lỗi khi debug: ${error.message}`);
    }
  }

  // Test sync 1 ca để debug
  async testSyncOneCase(caseData: { caseCode: string; serviceType: string; serviceName: string; source: string; receivedAt?: string }): Promise<any> {
    const { caseCode, serviceType, serviceName, source, receivedAt } = caseData;
    const logs: string[] = [];

    try {
      logs.push(`=== Testing case ${caseCode} ===`);
      logs.push(`ServiceType: ${serviceType}, ServiceName: ${serviceName}, Source: ${source}`);

      // Check if already processed
      const existingHistory = await this.historyModel
        .findOne({
          caseCode: caseCode,
          loaiThayDoi: HistoryType.NHAN_MAU_VE,
        })
        .exec();

      if (existingHistory) {
        logs.push(`⚠️ Case already processed (history ID: ${existingHistory._id})`);
        return { success: false, message: 'Case already processed', logs };
      }

      // Find supplies
      const supplies = await this.supplyModel
        .find({ loaiVatTu: serviceType })
        .sort({ createdAt: 1 })
        .exec();

      logs.push(`Found ${supplies.length} supplies for type ${serviceType}:`);
      supplies.forEach(s => logs.push(`  - ${s.tenVatTu} (${s._id})`));

      if (supplies.length === 0) {
        logs.push(`❌ No supplies found for type ${serviceType}`);
        return { success: false, message: 'No supplies found', logs };
      }

      // Find clinic
      let clinic = await this.clinicModel.findOne({ tenPhongKham: source }).exec();
      if (!clinic) {
        logs.push(`⚠️ Clinic "${source}" not found, would create new one`);
        return { success: false, message: 'Clinic not found (would create in real sync)', logs };
      }

      logs.push(`Found clinic: ${clinic.tenPhongKham} (${clinic._id})`);

      // Find allocation
      const targetAllocation = await this.allocationModel
        .findOne({
          phongKham: clinic._id,
          trangThai: AllocationStatus.DA_GIAO,
        })
        .sort({ ngayGiao: -1 })
        .exec();

      if (!targetAllocation) {
        logs.push(`⚠️ No delivered allocation found for clinic ${clinic.tenPhongKham}`);
        return { success: false, message: 'No allocation found', logs };
      }

      logs.push(`Found allocation: ${targetAllocation.maPhieu}`);
      logs.push(`Allocation has ${targetAllocation.danhSachVatTu.length} supplies:`);
      targetAllocation.danhSachVatTu.forEach(item => {
        logs.push(`  - ${item.tenVatTu} (${item.vatTu}): soLuong=${item.soLuong}, daNhan=${item.soLuongDaNhan || 0}`);
      });

      // Try to update
      logs.push(`\nAttempting to update soLuongDaNhan...`);
      let updatedCount = 0;
      const updates: any[] = [];

      for (const supply of supplies) {
        const supplyItem = targetAllocation.danhSachVatTu.find(
          (item) => item.vatTu.toString() === supply._id.toString()
        );

        if (supplyItem) {
          const oldValue = supplyItem.soLuongDaNhan || 0;
          supplyItem.soLuongDaNhan = oldValue + 1;
          logs.push(`  ✓ ${supply.tenVatTu}: ${oldValue} -> ${supplyItem.soLuongDaNhan}`);
          updates.push({ supply: supply.tenVatTu, old: oldValue, new: supplyItem.soLuongDaNhan });
          updatedCount++;
        } else {
          logs.push(`  ✗ ${supply.tenVatTu} NOT FOUND in allocation`);
        }
      }

      if (updatedCount > 0) {
        targetAllocation.markModified('danhSachVatTu');
        await targetAllocation.save();
        logs.push(`\n✓ Saved allocation with ${updatedCount} updates`);

        // Verify save
        const verifyAllocation = await this.allocationModel.findById(targetAllocation._id).exec();
        logs.push(`\nVerification after save:`);
        verifyAllocation.danhSachVatTu.forEach(item => {
          const matchingSupply = supplies.find(s => s._id.toString() === item.vatTu.toString());
          if (matchingSupply) {
            logs.push(`  - ${item.tenVatTu}: soLuongDaNhan = ${item.soLuongDaNhan || 0}`);
          }
        });

        return {
          success: true,
          message: `Updated ${updatedCount} supplies`,
          updates,
          logs,
        };
      } else {
        logs.push(`\n❌ No supplies matched in allocation`);
        return { success: false, message: 'No matching supplies', logs };
      }
    } catch (error) {
      logs.push(`\n❌ Error: ${error.message}`);
      return { success: false, error: error.message, logs };
    }
  }

  // Debug: Kiểm tra phiếu cấp
  async debugCheckAllocation(maPhieu: string): Promise<any> {
    try {
      const allocation = await this.allocationModel
        .findOne({ maPhieu })
        .populate('phongKham')
        .populate('nguoiTaoPhieu', 'hoTen')
        .exec();

      if (!allocation) {
        return { found: false, message: 'Allocation not found' };
      }

      // Get supplies with loaiVatTu
      const suppliesWithType = await Promise.all(
        allocation.danhSachVatTu.map(async (item) => {
          const supply = await this.supplyModel.findById(item.vatTu).exec();
          return {
            vatTu: item.vatTu,
            tenVatTu: item.tenVatTu,
            loaiVatTu: supply?.loaiVatTu || 'N/A',
            soLuong: item.soLuong,
            soLuongDaNhan: item.soLuongDaNhan || 0,
            tonKho: item.soLuong - (item.soLuongDaNhan || 0),
          };
        })
      );

      // Get related histories
      const histories = await this.historyModel
        .find({
          phieuCapPhat: allocation._id,
          loaiThayDoi: HistoryType.NHAN_MAU_VE,
        })
        .sort({ thoiGian: -1 })
        .exec();

      return {
        found: true,
        allocation: {
          _id: allocation._id,
          maPhieu: allocation.maPhieu,
          trangThai: allocation.trangThai,
          phongKham: {
            _id: (allocation.phongKham as any)?._id,
            tenPhongKham: (allocation.phongKham as any)?.tenPhongKham,
          },
          ngayGiao: allocation.ngayGiao,
          danhSachVatTu: suppliesWithType,
        },
        histories: histories.map(h => ({
          loaiVatTu: h.loaiVatTu,
          soLuong: h.soLuong,
          thoiGian: h.thoiGian,
          lyDo: h.lyDo,
          caseCode: (h as any).caseCode,
        })),
      };
    } catch (error) {
      throw new BadRequestException(`Lỗi khi kiểm tra: ${error.message}`);
    }
  }

  // Debug: Kiểm tra báo cáo tồn kho cho phòng khám
  async debugInventoryForClinic(clinicName: string): Promise<any> {
    try {
      // Find clinic
      const clinic = await this.clinicModel.findOne({ tenPhongKham: clinicName }).exec();
      if (!clinic) {
        return { found: false, message: 'Clinic not found' };
      }

      const clinicId = clinic._id.toString();

      // Get all allocations for this clinic
      const allocations = await this.allocationModel
        .find({
          phongKham: clinicId,
          trangThai: AllocationStatus.DA_GIAO,
        })
        .exec();

      // Get all sample return history for this clinic
      const histories = await this.historyModel
        .find({
          loaiThayDoi: HistoryType.NHAN_MAU_VE,
        })
        .populate('phieuCapPhat')
        .exec();

      const relevantHistories = histories.filter(h => {
        if (h.phieuCapPhat) {
          const alloc = allocations.find(a => a._id.toString() === (h.phieuCapPhat as any)._id?.toString() || h.phieuCapPhat.toString());
          return !!alloc;
        }
        return false;
      });

      // Build report map
      const reportMap: Map<string, any> = new Map();

      // Process allocations
      for (const allocation of allocations) {
        for (const supplyItem of allocation.danhSachVatTu) {
          const supply = await this.supplyModel.findById(supplyItem.vatTu).exec();
          if (!supply) continue;

          const supplyId = supply._id.toString();
          const key = `${clinicId}_${supplyId}`;

          if (!reportMap.has(key)) {
            reportMap.set(key, {
              tenVatTu: supply.tenVatTu,
              loaiVatTu: supply.loaiVatTu,
              soLuongCap: 0,
              soLuongDaDung: 0,
              allocations: [],
            });
          }

          const entry = reportMap.get(key);
          entry.soLuongCap += supplyItem.soLuong;
          entry.soLuongDaDung += supplyItem.soLuongDaNhan || 0;
          entry.allocations.push({
            maPhieu: allocation.maPhieu,
            soLuong: supplyItem.soLuong,
            soLuongDaNhan: supplyItem.soLuongDaNhan || 0,
          });
        }
      }

      // Process histories
      const historyDetails = relevantHistories.map(h => ({
        vatTu: h.vatTu?.toString(),
        loaiVatTu: h.loaiVatTu || 'N/A',
        soLuong: h.soLuong,
        thoiGian: h.thoiGian,
        lyDo: h.lyDo,
      }));

      return {
        found: true,
        clinic: {
          _id: clinic._id,
          tenPhongKham: clinic.tenPhongKham,
        },
        allocationsCount: allocations.length,
        historiesCount: relevantHistories.length,
        reportMap: Array.from(reportMap.entries()).map(([key, value]) => ({
          key,
          ...value,
          soLuongTon: value.soLuongCap - value.soLuongDaDung,
        })),
        histories: historyDetails,
      };
    } catch (error) {
      throw new BadRequestException(`Lỗi khi kiểm tra: ${error.message}`);
    }
  }
}