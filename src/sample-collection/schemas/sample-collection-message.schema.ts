import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';

@Schema({ timestamps: true })
export class SampleCollectionMessage extends Document {
  @Prop({ type: Types.ObjectId, ref: 'SampleCollection', required: true })
  sampleCollectionId: Types.ObjectId;

  @Prop({ type: Types.ObjectId, ref: 'User', required: true })
  userId: Types.ObjectId;

  @Prop({ required: true })
  message: string;

  @Prop({ type: [String], default: [] })
  attachments?: string[];
}

export const SampleCollectionMessageSchema = SchemaFactory.createForClass(SampleCollectionMessage);
