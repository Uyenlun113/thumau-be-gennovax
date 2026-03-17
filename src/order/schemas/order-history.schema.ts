import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';
import { OrderStatus } from './order.schema';

@Schema({ timestamps: true })
export class OrderHistory extends Document {
  @Prop({ type: Types.ObjectId, ref: 'Order', required: true })
  orderId: Types.ObjectId;

  @Prop({ required: true, enum: OrderStatus })
  trangThai: OrderStatus;

  @Prop({ type: Types.ObjectId, ref: 'User' })
  nguoiThucHien?: Types.ObjectId; // Người thực hiện hành động này

  @Prop()
  ghiChu?: string;

  @Prop()
  thoiGian: Date;

  @Prop({ type: Object })
  duLieuThayDoi?: any; // Lưu dữ liệu thay đổi (JSON)
}

export const OrderHistorySchema = SchemaFactory.createForClass(OrderHistory);
