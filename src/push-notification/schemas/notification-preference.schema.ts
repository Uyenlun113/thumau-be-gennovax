import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';

@Schema({ timestamps: true })
export class NotificationPreference extends Document {
  @Prop({ type: Types.ObjectId, ref: 'User', required: true, unique: true })
  userId: Types.ObjectId;

  @Prop({ type: Map, of: Boolean, default: {} })
  enabledTypes: Map<string, boolean>;

  @Prop({ default: true })
  pushEnabled: boolean;

  @Prop({ default: true })
  soundEnabled: boolean;

  @Prop({ default: true })
  badgeEnabled: boolean;
}

export const NotificationPreferenceSchema = SchemaFactory.createForClass(NotificationPreference);
