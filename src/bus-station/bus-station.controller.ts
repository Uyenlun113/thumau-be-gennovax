import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { ApiOperation, ApiQuery, ApiTags } from '@nestjs/swagger';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { BusStationService } from './bus-station.service';
import { CreateBusStationDto } from './dto/create-bus-station.dto';
import { UpdateBusStationDto } from './dto/update-bus-station.dto';

@ApiTags('Bus Stations')
@Controller('bus-stations')
@UseGuards(JwtAuthGuard)
export class BusStationController {
  constructor(private readonly busStationService: BusStationService) {}

  @Post()
  @ApiOperation({ summary: 'Tạo nhà xe mới' })
  create(@Body() createBusStationDto: CreateBusStationDto) {
    return this.busStationService.create(createBusStationDto);
  }

  @Get()
  @ApiOperation({ summary: 'Lấy danh sách nhà xe' })
  @ApiQuery({ name: 'page', required: false, type: Number })
  @ApiQuery({ name: 'limit', required: false, type: Number })
  @ApiQuery({ name: 'search', required: false, type: String })
  findAll(
    @Query('page') page?: number,
    @Query('limit') limit?: number,
    @Query('search') search?: string,
  ) {
    return this.busStationService.findAll({ page, limit, search });
  }

  @Get(':id')
  @ApiOperation({ summary: 'Lấy thông tin nhà xe theo ID' })
  findOne(@Param('id') id: string) {
    return this.busStationService.findOne(id);
  }

  @Patch(':id')
  @ApiOperation({ summary: 'Cập nhật thông tin nhà xe' })
  update(@Param('id') id: string, @Body() updateBusStationDto: UpdateBusStationDto) {
    return this.busStationService.update(id, updateBusStationDto);
  }

  @Delete(':id')
  @ApiOperation({ summary: 'Xóa nhà xe' })
  remove(@Param('id') id: string) {
    return this.busStationService.remove(id);
  }
}
