import { Controller, Get, Post, Put, Delete, Body, Param, UseGuards } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiParam, ApiBearerAuth } from '@nestjs/swagger';
import { WorkContentService } from './work-content.service';
import { CreateWorkContentDto } from './dto/create-work-content.dto';
import { UpdateWorkContentDto } from './dto/update-work-content.dto';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { PermissionsGuard } from '../auth/guards/permissions.guard';
import { Permissions } from '../auth/decorators/permissions.decorator';
import { Permission } from '../role/schemas/role.schema';

@ApiTags('work-contents')
@ApiBearerAuth('JWT-auth')
@Controller('work-contents')
@UseGuards(JwtAuthGuard, PermissionsGuard)
export class WorkContentController {
  constructor(private readonly workContentService: WorkContentService) {}

  @Post()
  @Permissions(Permission.WORK_CONTENT_CREATE)
  @ApiOperation({ summary: 'Tạo nội dung công việc mới' })
  @ApiResponse({ status: 201, description: 'Nội dung công việc đã được tạo thành công' })
  @ApiResponse({ status: 400, description: 'Dữ liệu không hợp lệ' })
  create(@Body() data: CreateWorkContentDto) {
    return this.workContentService.create(data);
  }

  @Get()
  @Permissions(Permission.WORK_CONTENT_VIEW)
  @ApiOperation({ summary: 'Lấy danh sách tất cả nội dung công việc' })
  @ApiResponse({ status: 200, description: 'Danh sách nội dung công việc' })
  findAll() {
    return this.workContentService.findAll();
  }

  @Get(':id')
  @Permissions(Permission.WORK_CONTENT_VIEW)
  @ApiOperation({ summary: 'Lấy thông tin nội dung công việc theo ID' })
  @ApiParam({ name: 'id', description: 'ID nội dung công việc' })
  @ApiResponse({ status: 200, description: 'Thông tin nội dung công việc' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy nội dung công việc' })
  findOne(@Param('id') id: string) {
    return this.workContentService.findOne(id);
  }

  @Put(':id')
  @Permissions(Permission.WORK_CONTENT_UPDATE)
  @ApiOperation({ summary: 'Cập nhật thông tin nội dung công việc' })
  @ApiParam({ name: 'id', description: 'ID nội dung công việc' })
  @ApiResponse({ status: 200, description: 'Nội dung công việc đã được cập nhật' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy nội dung công việc' })
  update(@Param('id') id: string, @Body() data: UpdateWorkContentDto) {
    return this.workContentService.update(id, data);
  }

  @Delete(':id')
  @Permissions(Permission.WORK_CONTENT_DELETE)
  @ApiOperation({ summary: 'Xóa nội dung công việc' })
  @ApiParam({ name: 'id', description: 'ID nội dung công việc' })
  @ApiResponse({ status: 200, description: 'Nội dung công việc đã được xóa' })
  @ApiResponse({ status: 404, description: 'Không tìm thấy nội dung công việc' })
  delete(@Param('id') id: string) {
    return this.workContentService.delete(id);
  }
}
