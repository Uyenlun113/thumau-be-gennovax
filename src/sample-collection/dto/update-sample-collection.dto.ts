import { PartialType } from '@nestjs/swagger';
import { CreateSampleCollectionDto } from './create-sample-collection.dto';

export class UpdateSampleCollectionDto extends PartialType(CreateSampleCollectionDto) {}
