import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

@Schema({ timestamps: true })
export class BusStation extends Document {
  @Prop({ required: true, unique: true })
  maNhaXe: string;

  @Prop({ required: true })
  tenNhaXe: string;

  @Prop({ required: true })
  diaChi: string;

  @Prop({ type: { lat: Number, lng: Number } })
  toaDo?: {
    lat: number;
    lng: number;
  };

  @Prop()
  soDienThoai?: string;

  @Prop()
  email?: string;

  @Prop()
  nguoiLienHe?: string;

  @Prop({ default: true })
  dangHoatDong: boolean;

  @Prop()
  ghiChu?: string;
}

export const BusStationSchema = SchemaFactory.createForClass(BusStation);
