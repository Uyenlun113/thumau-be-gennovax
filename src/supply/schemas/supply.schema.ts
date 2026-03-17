import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type SupplyDocument = Supply & Document;

export enum SupplyStatus {
  BINH_THUONG = 'BINH_THUONG', // Bình thường
  CAN_NHAP_THEM = 'CAN_NHAP_THEM', // Cần nhập thêm (tồn kho < mức tối thiểu)
}

@Schema({ timestamps: true })
export class Supply {
  @Prop({ required: true, unique: true })
  maVatTu: string; // Mã vật tư (VD: BCT-EV)

  @Prop({ required: true })
  tenVatTu: string; // Tên vật tư

  @Prop()
  moTa: string; // Mô tả chi tiết

  @Prop()
  donVi: string; // Đơn vị (Bộ, cái, ống, hộp...)

  @Prop({ default: 0 })
  tonKho: number; // Số lượng tồn kho hiện tại

  @Prop({ default: 0 })
  mucToiThieu: number; // Mức tối thiểu cảnh báo

  @Prop()
  hinhAnh: string; // URL hình ảnh vật tư

  @Prop({ 
    type: String, 
    enum: SupplyStatus, 
    default: SupplyStatus.BINH_THUONG 
  })
  trangThai: SupplyStatus;

  @Prop()
  createdAt: Date;

  @Prop()
  updatedAt: Date;
}

export const SupplySchema = SchemaFactory.createForClass(Supply);
