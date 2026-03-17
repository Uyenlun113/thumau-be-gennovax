import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';

@Schema({ timestamps: true })
export class WorkSchedule extends Document {
  @Prop({ type: Types.ObjectId, ref: 'User', required: true })
  userId: Types.ObjectId;

  @Prop({ required: true })
  date: string; // Format: YYYY-MM-DD

  @Prop({ type: String, required: true })
  caLamViec: string; // Single shift value: 'CA_1', 'CA_2', 'CA_1_CA_2', 'FULL_CA', 'OFF', etc.
}

export const WorkScheduleSchema = SchemaFactory.createForClass(WorkSchedule);

// Tạo index unique cho userId + date để tránh trùng lặp
WorkScheduleSchema.index({ userId: 1, date: 1 }, { unique: true });
