import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

@Schema({ timestamps: true })
export class Clinic extends Document {
  @Prop({ required: true, unique: true })
  maPhongKham: string;

  @Prop({ required: true })
  tenPhongKham: string;

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
  nguoiDaiDien?: string;

  @Prop()
  chuyenKhoa?: string;

  @Prop({ default: true })
  dangHoatDong: boolean;

  @Prop()
  ghiChu?: string;

  @Prop({ default: false })
  tuDongTaoLenh?: boolean;

  @Prop({
    type: [{
      ngayTaoLenhTrongTuan: [Number],
      noiDungCongViecMacDinh: String,
      ghiChuLenh: String,
      lenhUuTien: Boolean,
    }],
    default: []
  })
  cauHinhTuDongTaoLenh?: Array<{
    ngayTaoLenhTrongTuan: number[];
    noiDungCongViecMacDinh?: string;
    ghiChuLenh?: string;
    lenhUuTien?: boolean;
  }>;

  @Prop()
  nhanVienPhuTrach?: string;

  @Prop()
  sdtLienHe?: string;

  @Prop()
  linkZalo?: string;

  @Prop({
    type: [{
      day: Number, // 0=CN, 1=T2, 2=T3, 3=T4, 4=T5, 5=T6, 6=T7
      isOpen: Boolean,
      openTime: String, // Format: "08:00"
      closeTime: String, // Format: "17:00"
    }],
    default: [
      { day: 1, isOpen: true, openTime: '08:00', closeTime: '17:00' },
      { day: 2, isOpen: true, openTime: '08:00', closeTime: '17:00' },
      { day: 3, isOpen: true, openTime: '08:00', closeTime: '17:00' },
      { day: 4, isOpen: true, openTime: '08:00', closeTime: '17:00' },
      { day: 5, isOpen: true, openTime: '08:00', closeTime: '17:00' },
      { day: 6, isOpen: true, openTime: '08:00', closeTime: '12:00' },
      { day: 0, isOpen: false, openTime: '', closeTime: '' },
    ]
  })
  gioLamViec?: Array<{
    day: number;
    isOpen: boolean;
    openTime: string;
    closeTime: string;
  }>;

  // Deprecated - kept for backward compatibility
  @Prop()
  gioMoCua?: string;

  @Prop()
  gioDongCua?: string;

  @Prop({ type: [Number] })
  ngayLamViec?: number[];
}

export const ClinicSchema = SchemaFactory.createForClass(Clinic);
