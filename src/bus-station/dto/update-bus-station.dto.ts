import { PartialType } from '@nestjs/swagger';
import { CreateBusStationDto } from './create-bus-station.dto';

export class UpdateBusStationDto extends PartialType(CreateBusStationDto) {}
