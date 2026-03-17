import { Injectable, CanActivate, ExecutionContext, ForbiddenException, Logger } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { Permission } from '../../role/schemas/role.schema';

@Injectable()
export class PermissionsGuard implements CanActivate {
  private readonly logger = new Logger(PermissionsGuard.name);

  constructor(private reflector: Reflector) {}

  /**
   * Permission dependencies - some permissions automatically grant read access to related resources
   * This prevents permission conflicts where a feature needs to read data from another module
   */
  private readonly permissionDependencies: Record<string, Permission[]> = {
    // Schedule management needs to read users
    [Permission.SCHEDULE_VIEW]: [Permission.EMPLOYEE_VIEW],
    [Permission.SCHEDULE_CREATE]: [Permission.EMPLOYEE_VIEW],
    [Permission.SCHEDULE_UPDATE]: [Permission.EMPLOYEE_VIEW],
    [Permission.SCHEDULE_ASSIGN]: [Permission.EMPLOYEE_VIEW],
    
    // Order management needs to read users, clinics, and work content
    [Permission.ORDER_VIEW]: [Permission.EMPLOYEE_VIEW, Permission.CLINIC_VIEW, Permission.WORK_CONTENT_VIEW],
    [Permission.ORDER_CREATE]: [Permission.EMPLOYEE_VIEW, Permission.CLINIC_VIEW, Permission.WORK_CONTENT_VIEW],
    [Permission.ORDER_UPDATE]: [Permission.EMPLOYEE_VIEW, Permission.CLINIC_VIEW, Permission.WORK_CONTENT_VIEW],
    [Permission.ORDER_ASSIGN]: [Permission.EMPLOYEE_VIEW, Permission.CLINIC_VIEW],
    [Permission.ORDER_DETAIL_VIEW]: [Permission.EMPLOYEE_VIEW, Permission.CLINIC_VIEW, Permission.WORK_CONTENT_VIEW],
    
    // Allocation management needs to read clinics and supplies
    [Permission.ALLOCATION_VIEW]: [Permission.CLINIC_VIEW, Permission.SUPPLY_VIEW],
    [Permission.ALLOCATION_CREATE]: [Permission.CLINIC_VIEW, Permission.SUPPLY_VIEW],
    [Permission.ALLOCATION_UPDATE]: [Permission.CLINIC_VIEW, Permission.SUPPLY_VIEW],
    [Permission.ALLOCATION_DETAIL_VIEW]: [Permission.CLINIC_VIEW, Permission.SUPPLY_VIEW],
    
    // Dashboard and reports need to read everything
    [Permission.DASHBOARD_VIEW]: [
      Permission.ORDER_VIEW,
      Permission.EMPLOYEE_VIEW,
      Permission.CLINIC_VIEW,
      Permission.SUPPLY_VIEW,
      Permission.WORK_CONTENT_VIEW,
    ],
    [Permission.REPORT_VIEW]: [
      Permission.ORDER_VIEW,
      Permission.EMPLOYEE_VIEW,
      Permission.CLINIC_VIEW,
      Permission.SUPPLY_VIEW,
      Permission.ALLOCATION_VIEW,
    ],
    [Permission.STATISTICS_VIEW]: [
      Permission.ORDER_VIEW,
      Permission.EMPLOYEE_VIEW,
      Permission.CLINIC_VIEW,
      Permission.SUPPLY_VIEW,
    ],
  };

  /**
   * Get all permissions including dependencies
   */
  private getEffectivePermissions(userPermissions: Permission[]): Set<Permission> {
    const effectivePermissions = new Set<Permission>(userPermissions);
    
    // Add dependent permissions
    for (const permission of userPermissions) {
      const dependencies = this.permissionDependencies[permission];
      if (dependencies) {
        dependencies.forEach(dep => effectivePermissions.add(dep));
      }
    }
    
    return effectivePermissions;
  }

  canActivate(context: ExecutionContext): boolean {
    const requiredPermissions = this.reflector.getAllAndOverride<Permission[]>('permissions', [
      context.getHandler(),
      context.getClass(),
    ]);

    if (!requiredPermissions) {
      return true; // No permissions required
    }

    const request = context.switchToHttp().getRequest();
    const user = request.user;

    this.logger.debug(`Required permissions: ${JSON.stringify(requiredPermissions)}`);
    this.logger.debug(`User: ${JSON.stringify(user?._id)}`);
    this.logger.debug(`User permissions: ${JSON.stringify(user?.permissions)}`);

    if (!user) {
      this.logger.warn('No user in request');
      throw new ForbiddenException('Không có quyền truy cập');
    }

    // Nếu user không có permissions array (legacy users), cho phép truy cập
    if (!user.permissions || !Array.isArray(user.permissions)) {
      this.logger.warn(`User ${user._id} has no permissions array, allowing access`);
      return true;
    }

    // Get effective permissions including dependencies
    const effectivePermissions = this.getEffectivePermissions(user.permissions);
    
    this.logger.debug(`Effective permissions: ${JSON.stringify(Array.from(effectivePermissions))}`);

    // Kiểm tra xem user có quyền yêu cầu không (bao gồm cả quyền phụ thuộc)
    const hasPermission = requiredPermissions.some((permission) =>
      effectivePermissions.has(permission),
    );

    if (!hasPermission) {
      this.logger.warn(`User ${user._id} missing required permissions. Has: ${user.permissions.join(', ')}, Needs: ${requiredPermissions.join(', ')}`);
      throw new ForbiddenException('Bạn không có quyền thực hiện hành động này');
    }

    this.logger.debug(`User ${user._id} has required permissions`);
    return true;
  }
}
