import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { BusStationController } from './bus-station.controller';
import { BusStationService } from './bus-station.service';
import { BusStation, BusStationSchema } from './schemas/bus-station.schema';

@Module({
  imports: [
    MongooseModule.forFeature([{ name: BusStation.name, schema: BusStationSchema }]),
  ],
  controllers: [BusStationController],
  providers: [BusStationService],
  exports: [BusStationService],
})
export class BusStationModule {}
