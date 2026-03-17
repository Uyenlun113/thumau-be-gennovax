import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import * as bcrypt from 'bcrypt';
import { Document, Types } from 'mongoose';

export enum CaLamViec {
  FULL_CA = 'FULL_CA',           // Full ca (08:00 - 17:00)
  CA_1 = 'CA_1',                 // Ca 1 (08:00 - 12:00)
  CA_2 = 'CA_2',                 // Ca 2 (13:00 - 17:00)
  CA_3 = 'CA_3',                 // Ca 3 (17:00 - 21:00)
  CA_1_CA_2 = 'CA_1_CA_2',       // Ca 1, Ca 2 (Cả ngày - làm nhiều hơn 1 ca)
  CA_2_CA_3 = 'CA_2_CA_3',       // Ca 2, Ca 3 (Chiều + Tối)
  CA_1_CA_3 = 'CA_1_CA_3',       // Ca 1, Ca 3 (Sáng + Tối)
  CA_1_CA_2_CA_3 = 'CA_1_CA_2_CA_3', // Ca 1, Ca 2, Ca 3 (Cả ngày + Tối)
  MOT_PHAN_2_SANG = 'MOT_PHAN_2_SANG',   // 1/2P (S) - Nghỉ có phép buổi sáng, làm việc: 13:00 - 17:00
  MOT_PHAN_2_CHIEU = 'MOT_PHAN_2_CHIEU', // 1/2P (C) - Nghỉ có phép buổi chiều, làm việc: 08:00 - 12:00
  OFF_SANG = 'OFF_SANG',         // OFF (S) - Nghỉ buổi sáng (không trừ phép), làm việc: 13:00 - 17:00
  OFF_CHIEU = 'OFF_CHIEU',       // OFF (C) - Nghỉ buổi chiều (không trừ phép), làm việc: 08:00 - 12:00
  MOT_PHAN = 'MOT_PHAN',         // 1P - Nghỉ có phép cả ngày
  OFF = 'OFF',                   // OFF - Nghỉ nguyên ngày (cuối tuần, nghỉ bù,... không trừ phép)
  NGHI_LE = 'NGHI_LE',           // L - Nghỉ lễ / tết
}

@Schema({ timestamps: true })
export class User extends Document {
  @Prop({ required: true, unique: true })
  maNhanVien: string;

  @Prop({ required: true, unique: true })
  username: string;

  @Prop({ required: true })
  password: string;

  @Prop({ required: true })
  hoTen: string;

  @Prop({ type: Types.ObjectId, ref: 'Role', required: true })
  vaiTro: Types.ObjectId;

  @Prop()
  email: string;

  @Prop()
  soDienThoai: string;

  @Prop({ type: { lat: Number, lng: Number } })
  viTriHienTai?: {
    lat: number;
    lng: number;
  };

  @Prop({ default: true })
  dangHoatDong: boolean;

  @Prop({ enum: CaLamViec, default: CaLamViec.FULL_CA })
  caLamViec: CaLamViec;
}

export const UserSchema = SchemaFactory.createForClass(User);

// Hash password before saving
UserSchema.pre('save', async function (next) {
  const user = this as any;

  // Only hash the password if it has been modified (or is new)
  if (!user.isModified('password')) {
    return next();
  }

  try {
    const salt = await bcrypt.genSalt(10);
    user.password = await bcrypt.hash(user.password, salt);
    next();
  } catch (error) {
    next(error);
  }
});
