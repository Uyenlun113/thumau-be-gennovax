import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Schema as MongooseSchema, Types } from 'mongoose';

export type SupplyAllocationDocument = SupplyAllocation & Document;

export enum AllocationStatus {
  CHO_CHUAN_BI = 'CHO_CHUAN_BI', // Chờ chuẩn bị
  CHUAN_BI_HANG = 'CHUAN_BI_HANG', // Chuẩn bị hàng
  DA_GIAO = 'DA_GIAO', // Đã giao
}

export enum DeliveryMethod {
  CAP_TAN_NOI = 'CAP_TAN_NOI', // Cấp tận nơi
  GUI_CHUYEN_PHAT = 'GUI_CHUYEN_PHAT', // Gửi chuyển phát
  GUI_XE_SHIP = 'GUI_XE_SHIP', // Gửi xe, Ship
}

export interface SupplyItem {
  vatTu: Types.ObjectId; // Reference to Supply
  tenVatTu: string; // Tên vật tư (lưu để hiển thị)
  soLuong: number; // Số lượng cấp
  soLuongDaNhan?: number; // Số lượng đã nhận về (từ phòng khám)
  hanSuDung?: Date; // Hạn sử dụng (tùy chọn)
}

// Define sub-schema for SupplyItem
const SupplyItemSchema = new MongooseSchema({
  vatTu: { type: MongooseSchema.Types.ObjectId, ref: 'Supply', required: true },
  tenVatTu: { type: String, required: true },
  soLuong: { type: Number, required: true },
  soLuongDaNhan: { type: Number, default: 0 },
  hanSuDung: { type: Date },
}, { _id: false });

@Schema({ timestamps: true })
export class SupplyAllocation {
  @Prop({ required: true, unique: true })
  maPhieu: string; // Mã phiếu (VD: PC060226140504)

  @Prop({ type: Types.ObjectId, ref: 'User', required: true })
  nguoiTaoPhieu: Types.ObjectId; // Người tạo phiếu

  @Prop({ type: Types.ObjectId, ref: 'Clinic', required: true })
  phongKham: Types.ObjectId; // Phòng khám nhận

  @Prop({
    type: String,
    enum: DeliveryMethod,
    required: true
  })
  hinhThucVanChuyen: DeliveryMethod;

  @Prop({ type: [SupplyItemSchema], required: true })
  danhSachVatTu: SupplyItem[]; // Danh sách vật tư cấp

  @Prop({
    type: String,
    enum: AllocationStatus,
    default: AllocationStatus.CHO_CHUAN_BI
  })
  trangThai: AllocationStatus;

  @Prop()
  ngayGiao: Date; // Ngày giao hàng thực tế

  @Prop()
  ghiChu: string; // Ghi chú khi giao hàng

  @Prop()
  nguoiNhan: string; // Người nhận hàng

  @Prop()
  diaChi: string; // Địa chỉ nhận hàng

  @Prop()
  soDienThoai: string; // Số điện thoại người nhận

  @Prop()
  anhGiaoNhan: string; // Ảnh giao nhận

  @Prop({ type: Types.ObjectId, ref: 'User' })
  nguoiGiaoHang: Types.ObjectId; // Người xác nhận giao hàng

  @Prop()
  thoiGianGiao: Date; // Thời gian xác nhận giao

  @Prop({ default: false })
  daGuiZalo: boolean; // Đã gửi tin nhắn Zalo

  @Prop()
  createdAt: Date;

  @Prop()
  updatedAt: Date;
}

export const SupplyAllocationSchema = SchemaFactory.createForClass(SupplyAllocation);
