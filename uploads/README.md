# Uploads Directory

Thư mục này chứa các file được upload lên server.

## Cấu trúc thư mục

```
uploads/
├── sample-collections/  # Hình ảnh hoàn thành lệnh nhận mẫu
└── README.md
```

## Quy định

- File ảnh tối đa: 5MB
- Định dạng chấp nhận: JPG, JPEG, PNG, GIF
- Tối đa 10 ảnh mỗi lần upload

## API Endpoint

### Upload hình ảnh hoàn thành

```
POST /sample-collections/:id/upload-images
Content-Type: multipart/form-data

Body:
- images: File[] (tối đa 10 files)
```

### Truy cập ảnh

Ảnh được lưu tại: `https://api.businessdigital-partners.com/uploads/sample-collections/{filename}`

## Lưu ý

- Thư mục này không được commit vào git (ngoại trừ file .gitkeep)
- Backup định kỳ thư mục này để tránh mất dữ liệu
