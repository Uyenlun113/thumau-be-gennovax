import { SetMetadata } from '@nestjs/common';
import { Permission } from '../../role/schemas/role.schema';

export const Permissions = (...permissions: Permission[]) => SetMetadata('permissions', permissions);
