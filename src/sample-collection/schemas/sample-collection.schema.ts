import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';

export enum SampleCollectionStatus {
  CHO_DIEU_PHOI = 'CHO_DIEU_PHOI',
  DANG_THUC_HIEN = 'DANG_THUC_HIEN',
  HOAN_THANH = 'HOAN_THANH',
  HOAN_THANH_KIEM_TRA = 'HOAN_THANH_KIEM_TRA',
  DA_HUY = 'DA_HUY',
}

@Schema({ timestamps: true })
export class SampleCollection extends Document {
  @Prop({ required: true, unique: true })
  maLenh: string;

  @Prop({ type: Types.ObjectId, ref: 'WorkContent', required: true })
  noiDungCongViec: Types.ObjectId;

  @Prop({ type: Types.ObjectId, ref: 'User', required: true })
  nguoiGiaoLenh: Types.ObjectId;

  @Prop({ type: Types.ObjectId, ref: 'User' })
  nhanVienThucHien?: Types.ObjectId;

  @Prop({ type: [String], default: [] })
  anhHoanThanh?: string[]; // Ảnh hoàn thành (bước HOAN_THANH)

  @Prop({ required: true, enum: SampleCollectionStatus, default: SampleCollectionStatus.CHO_DIEU_PHOI })
  trangThai: SampleCollectionStatus;

  @Prop()
  thoiGianHoanThanh?: Date;

  @Prop()
  thoiGianHoanThanhKiemTra?: Date;

  @Prop()
  emailSentAt?: Date; // Thời gian gửi email thông báo hoàn thành

  @Prop({ type: Types.ObjectId, ref: 'Clinic' })
  phongKhamKiemTra?: Types.ObjectId;

  @Prop()
  ghiChu?: string;

  @Prop({ type: { lat: Number, lng: Number } })
  viTri?: {
    lat: number;
    lng: number;
  };

  @Prop({ default: false })
  uuTien?: boolean;

  @Prop()
  thoiGianHenHoanThanh?: Date;

  @Prop({ type: Number, default: 0 })
  khoangCachDiChuyen?: number; // Khoảng cách từ nhân viên đến phòng khám (km)

  // Bus station order fields (optional - chỉ dùng cho lệnh nhà xe)
  @Prop()
  tenNhaXe?: string;

  @Prop()
  diaChiNhaXe?: string;

  // Danh sách phòng khám và chi phí
  // - Lệnh standard: 1 item duy nhất
  // - Lệnh bus station: nhiều items
  @Prop({
    type: [{
      phongKham: { type: Types.ObjectId, ref: 'Clinic', required: true },
      soTienCuocNhanMau: { type: Number, default: 0 },
      soTienShip: { type: Number, default: 0 },
      soTienGuiXe: { type: Number, default: 0 },
      anhHoanThanhKiemTra: { type: [String], default: [] }, // Ảnh hoàn thành kiểm tra (bước HOAN_THANH_KIEM_TRA)
      thoiGianHoanThanhKiemTra: { type: Date } // Thời gian hoàn thành kiểm tra
    }],
    default: []
  })
  phongKhamItems: Array<{
    phongKham: Types.ObjectId;
    soTienCuocNhanMau: number;
    soTienShip: number;
    soTienGuiXe: number;
    anhHoanThanhKiemTra: string[];
    thoiGianHoanThanhKiemTra?: Date;
  }>;
}

export const SampleCollectionSchema = SchemaFactory.createForClass(SampleCollection);
