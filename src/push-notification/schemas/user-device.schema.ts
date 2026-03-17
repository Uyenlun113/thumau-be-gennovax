import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';

@Schema({ timestamps: true })
export class UserDevice extends Document {
  @Prop({ type: Types.ObjectId, ref: 'User', required: true, index: true })
  userId: Types.ObjectId;

  @Prop({ required: true })
  playerId: string; // OneSignal Player ID

  @Prop({ type: String, enum: ['ios', 'android', 'web'], required: true })
  platform: 'ios' | 'android' | 'web';

  @Prop()
  deviceModel?: string;

  @Prop()
  appVersion?: string;

  @Prop()
  osVersion?: string;

  @Prop({ default: true })
  isActive: boolean;

  @Prop()
  lastUsedAt: Date;
}

export const UserDeviceSchema = SchemaFactory.createForClass(UserDevice);

// Indexes
UserDeviceSchema.index({ userId: 1, playerId: 1 }, { unique: true });
UserDeviceSchema.index({ isActive: 1 });
