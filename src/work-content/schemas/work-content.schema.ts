import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

@Schema({ timestamps: true })
export class WorkContent extends Document {
  @Prop({ required: true })
  tenCongViec: string;

  @Prop()
  moTa: string;
}

export const WorkContentSchema = SchemaFactory.createForClass(WorkContent);
