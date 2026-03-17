# Swagger API Documentation

## Truy cập Swagger UI

Sau khi khởi động backend, bạn có thể truy cập Swagger documentation tại:

```
http://localhost:5001/api-docs
```

## Khởi động Backend

```bash
# Development mode
npm run start:dev

# Production mode
npm run start:prod
```

## API Endpoints Overview

### 1. Clinics (Phòng khám)
- `POST /clinics` - Tạo phòng khám mới
- `GET /clinics` - Lấy danh sách tất cả phòng khám
- `GET /clinics/nearby?lat={lat}&lng={lng}&maxDistance={km}` - Tìm phòng khám gần vị trí
- `GET /clinics/code/:maPhongKham` - Tìm phòng khám theo mã
- `GET /clinics/:id` - Lấy thông tin phòng khám theo ID
- `PUT /clinics/:id` - Cập nhật thông tin phòng khám
- `DELETE /clinics/:id` - Xóa phòng khám

### 2. Orders (Phiếu cấp vật tư)
- `POST /orders` - Tạo phiếu cấp mới
- `GET /orders?status={status}` - Lấy danh sách phiếu cấp (có thể lọc theo trạng thái)
- `GET /orders/stats/summary` - Lấy thống kê tổng quan
- `GET /orders/:id` - Lấy thông tin phiếu cấp theo ID
- `PUT /orders/:id` - Cập nhật thông tin phiếu cấp
- `PUT /orders/:id/assign` - Phân công người thực hiện
- `PUT /orders/:id/status` - Cập nhật trạng thái phiếu cấp
- `DELETE /orders/:id` - Xóa phiếu cấp

### 3. Users (Người dùng)
- `POST /users` - Tạo người dùng mới
- `GET /users` - Lấy danh sách tất cả người dùng
- `GET /users/nearest?lat={lat}&lng={lng}` - Tìm nhân viên gần nhất
- `GET /users/:id` - Lấy thông tin người dùng theo ID
- `PUT /users/:id` - Cập nhật thông tin người dùng
- `PUT /users/:id/location` - Cập nhật vị trí người dùng
- `DELETE /users/:id` - Xóa người dùng

### 4. Roles (Vai trò)
- `POST /roles` - Tạo vai trò mới
- `GET /roles` - Lấy danh sách tất cả vai trò
- `GET /roles/:id` - Lấy thông tin vai trò theo ID
- `PUT /roles/:id` - Cập nhật thông tin vai trò
- `DELETE /roles/:id` - Xóa vai trò

### 5. Sample Collections (Lệnh nhận mẫu)
- `POST /sample-collections` - Tạo lệnh nhận mẫu mới
- `GET /sample-collections?status={status}` - Lấy danh sách lệnh nhận mẫu
- `GET /sample-collections/stats/summary` - Lấy thống kê tổng quan
- `GET /sample-collections/staff/:staffId` - Lấy danh sách theo nhân viên
- `GET /sample-collections/clinic/:clinicId` - Lấy danh sách theo phòng khám
- `GET /sample-collections/code/:maLenh` - Tìm lệnh theo mã
- `GET /sample-collections/:id` - Lấy thông tin lệnh theo ID
- `PUT /sample-collections/:id` - Cập nhật thông tin lệnh
- `PUT /sample-collections/:id/assign` - Phân công nhân viên
- `PUT /sample-collections/:id/status` - Cập nhật trạng thái
- `DELETE /sample-collections/:id` - Xóa lệnh

### 6. Work Schedules (Lịch làm việc)
- `POST /work-schedules` - Thiết lập lịch làm việc
- `GET /work-schedules/month?year={year}&month={month}` - Lấy lịch theo tháng
- `GET /work-schedules/user/:userId?startDate={date}&endDate={date}` - Lấy lịch theo người dùng
- `GET /work-schedules/:userId/:date` - Lấy lịch theo ngày cụ thể
- `DELETE /work-schedules/:userId/:date` - Xóa lịch làm việc

### 7. Work Contents (Nội dung công việc)
- `POST /work-contents` - Tạo nội dung công việc mới
- `GET /work-contents` - Lấy danh sách tất cả nội dung công việc
- `GET /work-contents/:id` - Lấy thông tin nội dung công việc theo ID
- `PUT /work-contents/:id` - Cập nhật thông tin nội dung công việc
- `DELETE /work-contents/:id` - Xóa nội dung công việc

## Trạng thái (Status) Enums

### Order Status & Sample Collection Status
- `CHO_DIEU_PHOI` - Chờ điều phối
- `CHO_NHAN_LENH` - Chờ nhận lệnh
- `DANG_THUC_HIEN` - Đang thực hiện
- `HOAN_THANH` - Hoàn thành
- `DA_HUY` - Đã hủy

### Ca Làm Việc (Work Shift)
- `FULL_CA` - Full ca
- `CA_1` - Ca 1
- `CA_2` - Ca 2
- `CA_3` - Ca 3
- `CA_1_CA_2` - Ca 1 + Ca 2
- `MOT_PHAN_2_SANG` - Một phần 2 sáng
- `MOT_PHAN_2_CHIEU` - Một phần 2 chiều
- `OFF_SANG` - Off sáng
- `OFF_CHIEU` - Off chiều
- `MOT_PHAN` - Một phần
- `OFF` - Off
- `NGHI_LE` - Nghỉ lễ

## Ví dụ Request Body

### Tạo Phòng Khám
```json
{
  "maPhongKham": "PK001",
  "tenPhongKham": "Phòng khám Đa khoa ABC",
  "diaChi": "123 Nguyễn Văn Linh, Q7, TP.HCM",
  "toaDo": {
    "lat": 10.762622,
    "lng": 106.660172
  },
  "soDienThoai": "0901234567",
  "email": "contact@clinic.com",
  "nguoiDaiDien": "Nguyễn Văn A",
  "chuyenKhoa": "Nội khoa",
  "dangHoatDong": true
}
```

### Tạo Người Dùng
```json
{
  "maNhanVien": "NV001",
  "username": "user001",
  "password": "password123",
  "hoTen": "Nguyễn Văn A",
  "vaiTro": "507f1f77bcf86cd799439011",
  "email": "user@example.com",
  "soDienThoai": "0901234567",
  "caLamViec": "FULL_CA",
  "dangHoatDong": true
}
```

### Tạo Lệnh Nhận Mẫu
```json
{
  "maLenh": "ML001",
  "phongKham": "507f1f77bcf86cd799439011",
  "noiDungCongViec": "507f1f77bcf86cd799439012",
  "nguoiGiaoLenh": "507f1f77bcf86cd799439013",
  "soTienCuocNhanMau": 50010,
  "soTienShip": 30000,
  "soTienGuiXe": 10000,
  "trangThai": "CHO_DIEU_PHOI",
  "uuTien": false
}
```

## Testing với Swagger UI

1. Mở trình duyệt và truy cập `http://localhost:5001/api-docs`
2. Chọn endpoint muốn test
3. Click "Try it out"
4. Nhập dữ liệu vào các trường
5. Click "Execute" để gửi request
6. Xem response trả về

## Notes

- Tất cả các endpoint đều có validation và error handling
- Response codes:
  - 200: Success
  - 201: Created
  - 400: Bad Request
  - 404: Not Found
  - 500: Internal Server Error
