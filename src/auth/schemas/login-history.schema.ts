import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';

@Schema({ timestamps: true })
export class LoginHistory extends Document {
  @Prop({ type: Types.ObjectId, ref: 'User', required: true })
  userId: Types.ObjectId;

  @Prop({ required: true })
  loginTime: Date;

  @Prop()
  logoutTime?: Date;

  @Prop()
  ipAddress?: string;

  @Prop()
  userAgent?: string;

  @Prop()
  deviceType?: string; // mobile, tablet, desktop

  @Prop()
  browser?: string;

  @Prop()
  os?: string;

  @Prop()
  deviceName?: string;

  @Prop({ default: true })
  isActive: boolean; // Còn đang đăng nhập hay đã logout

  @Prop()
  location?: string; // Vị trí địa lý nếu có
}

export const LoginHistorySchema = SchemaFactory.createForClass(LoginHistory);
