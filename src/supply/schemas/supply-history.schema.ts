import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';

export type SupplyHistoryDocument = SupplyHistory & Document;

export enum HistoryType {
  NHAP_KHO = 'NHAP_KHO', // Nhập kho
  XUAT_CAP = 'XUAT_CAP', // Xuất cấp
  HOAN_KHO = 'HOAN_KHO', // Hoàn kho (do xóa phiếu)
  DIEU_CHINH = 'DIEU_CHINH', // Điều chỉnh kho
  NHAN_MAU_VE = 'NHAN_MAU_VE', // Nhận mẫu về từ phòng khám
}

@Schema({ timestamps: true })
export class SupplyHistory {
  @Prop({ type: Types.ObjectId, ref: 'Supply', required: true })
  vatTu: Types.ObjectId;

  @Prop({ type: String })
  loaiVatTu: string; // Loại vật tư (NIPT, ADN, CELL, HPV, KHAC) - dùng để nhóm

  @Prop({ 
    type: String, 
    enum: HistoryType, 
    required: true 
  })
  loaiThayDoi: HistoryType;

  @Prop({ required: true })
  soLuong: number; // Số lượng thay đổi (+ hoặc -)

  @Prop()
  lyDo: string; // Lý do thay đổi

  @Prop({ type: Types.ObjectId, ref: 'User' })
  nguoiThucHien: Types.ObjectId;

  @Prop({ type: Types.ObjectId, ref: 'SupplyAllocation' })
  phieuCapPhat: Types.ObjectId; // Liên kết đến phiếu cấp phát (nếu có)

  @Prop({ type: String })
  phongKham: string; // Mã phòng khám (dùng khi không có phiếu cấp phát)

  @Prop({ type: String })
  caseCode: string; // Mã ca (dùng để kiểm tra trùng lặp khi nhập mẫu từ API)

  @Prop()
  receivedAt: Date; // Ngày nhận mẫu từ API bên thứ 3

  @Prop({ required: true })
  thoiGian: Date;

  @Prop()
  createdAt: Date;
}

export const SupplyHistorySchema = SchemaFactory.createForClass(SupplyHistory);
