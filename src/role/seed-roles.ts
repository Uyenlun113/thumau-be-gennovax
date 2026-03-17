import { Permission } from './schemas/role.schema';

export const defaultRoles = [
  {
    tenVaiTro: 'Admin',
    moTa: 'Quản trị viên - Có toàn bộ quyền trong hệ thống',
    permissions: Object.values(Permission), // Tất cả quyền
  },
  {
    tenVaiTro: 'Quản lý',
    moTa: 'Quản lý - Có quyền quản lý nhân viên và xem báo cáo',
    permissions: [
      Permission.DASHBOARD_VIEW,
      Permission.ORDER_VIEW,
      Permission.ORDER_VIEW_ALL, // Quản lý xem tất cả lệnh
      Permission.ORDER_CREATE,
      Permission.ORDER_UPDATE,
      Permission.ORDER_ASSIGN,
      Permission.ORDER_DETAIL_VIEW,
      Permission.EMPLOYEE_VIEW,
      Permission.EMPLOYEE_DETAIL_VIEW,
      Permission.CLINIC_VIEW,
      Permission.CLINIC_DETAIL_VIEW,
      Permission.SUPPLY_VIEW,
      Permission.SUPPLY_DETAIL_VIEW,
      Permission.ALLOCATION_VIEW,
      Permission.ALLOCATION_CREATE,
      Permission.ALLOCATION_DETAIL_VIEW,
      Permission.SCHEDULE_VIEW,
      Permission.SCHEDULE_CREATE,
      Permission.SCHEDULE_UPDATE,
      Permission.SCHEDULE_ASSIGN,
      Permission.NOTIFICATION_VIEW,
      Permission.REPORT_VIEW,
      Permission.STATISTICS_VIEW,
    ],
  },
  {
    tenVaiTro: 'Nhân viên thu mẫu',
    moTa: 'Nhân viên thu mẫu - Chỉ xem và cập nhật lệnh thu mẫu',
    permissions: [
      Permission.DASHBOARD_VIEW,
      Permission.ORDER_VIEW,
      Permission.ORDER_VIEW_OWN, // Nhân viên chỉ xem lệnh của mình
      Permission.ORDER_UPDATE,
      Permission.ORDER_DETAIL_VIEW,
      Permission.SCHEDULE_VIEW,
      Permission.CLINIC_VIEW,
      Permission.CLINIC_DETAIL_VIEW,
      Permission.NOTIFICATION_VIEW,
    ],
  },
  {
    tenVaiTro: 'Nhân viên kho',
    moTa: 'Nhân viên kho - Quản lý vật tư và cấp phát',
    permissions: [
      Permission.DASHBOARD_VIEW,
      Permission.SUPPLY_VIEW,
      Permission.SUPPLY_CREATE,
      Permission.SUPPLY_UPDATE,
      Permission.SUPPLY_DETAIL_VIEW,
      Permission.ALLOCATION_VIEW,
      Permission.ALLOCATION_CREATE,
      Permission.ALLOCATION_UPDATE,
      Permission.ALLOCATION_DETAIL_VIEW,
      Permission.CLINIC_VIEW,
      Permission.CLINIC_DETAIL_VIEW,
      Permission.NOTIFICATION_VIEW,
    ],
  },
  {
    tenVaiTro: 'Nhân viên giao hàng',
    moTa: 'Nhân viên giao hàng - Xem và xác nhận giao hàng',
    permissions: [
      Permission.DASHBOARD_VIEW,
      Permission.ALLOCATION_VIEW,
      Permission.ALLOCATION_UPDATE,
      Permission.ALLOCATION_DETAIL_VIEW,
      Permission.CLINIC_VIEW,
      Permission.CLINIC_DETAIL_VIEW,
      Permission.NOTIFICATION_VIEW,
    ],
  },
];
