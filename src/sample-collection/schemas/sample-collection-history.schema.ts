import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';
import { SampleCollectionStatus } from './sample-collection.schema';

@Schema({ timestamps: true })
export class SampleCollectionHistory extends Document {
  @Prop({ type: Types.ObjectId, ref: 'SampleCollection', required: true })
  sampleCollectionId: Types.ObjectId;

  @Prop({ enum: SampleCollectionStatus })
  trangThaiTruoc?: SampleCollectionStatus; // Trạng thái trước đó

  @Prop({ required: true, enum: SampleCollectionStatus })
  trangThai: SampleCollectionStatus; // Trạng thái hiện tại (sau khi thay đổi)

  @Prop({ type: Types.ObjectId, ref: 'User' })
  nguoiThucHien?: Types.ObjectId; // Người thực hiện hành động này

  @Prop()
  ghiChu?: string;

  @Prop()
  thoiGian: Date;

  @Prop({ type: Object })
  duLieuThayDoi?: any; // Lưu dữ liệu thay đổi (JSON)
}

export const SampleCollectionHistorySchema = SchemaFactory.createForClass(SampleCollectionHistory);
