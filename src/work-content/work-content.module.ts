import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { WorkContentController } from './work-content.controller';
import { WorkContentService } from './work-content.service';
import { WorkContent, WorkContentSchema } from './schemas/work-content.schema';

@Module({
  imports: [
    MongooseModule.forFeature([{ name: WorkContent.name, schema: WorkContentSchema }]),
  ],
  controllers: [WorkContentController],
  providers: [WorkContentService],
  exports: [WorkContentService],
})
export class WorkContentModule {}
