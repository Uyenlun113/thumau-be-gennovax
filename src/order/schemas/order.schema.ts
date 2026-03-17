import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export enum OrderStatus {
  CHO_DIEU_PHOI = 'CHO_DIEU_PHOI',
  DANG_THUC_HIEN = 'DANG_THUC_HIEN',
  HOAN_THANH = 'HOAN_THANH',
  HOAN_THANH_KIEM_TRA = 'HOAN_THANH_KIEM_TRA',
  DA_HUY = 'DA_HUY',
}

@Schema({ timestamps: true })
export class Order extends Document {
  @Prop({ required: true, unique: true })
  maPhieuCap: string;

  @Prop({ required: true })
  phongKham: string;

  @Prop({ required: true })
  diaDiem: string;

  @Prop({ required: true })
  tuNgay: Date;

  @Prop({ required: true })
  denNgay: Date;

  @Prop({ required: true, enum: OrderStatus, default: OrderStatus.CHO_DIEU_PHOI })
  trangThai: OrderStatus;

  @Prop()
  tenVatTu: string;

  @Prop({ default: 0 })
  soLuongCap: number;

  @Prop({ default: 0 })
  soLuongDaDung: number;

  @Prop({ default: 0 })
  soLuongTon: number;

  @Prop({ default: 0 })
  tyLeSuDung: number;

  @Prop()
  ghiChu: string;

  @Prop()
  nguoiTao: string;

  @Prop()
  nguoiThucHien: string;

  @Prop()
  emailPhongKham: string;

  @Prop({ type: { lat: Number, lng: Number } })
  toaDo: {
    lat: number;
    lng: number;
  };
}

export const OrderSchema = SchemaFactory.createForClass(Order);
