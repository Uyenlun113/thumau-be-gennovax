import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as nodemailer from 'nodemailer';

@Injectable()
export class EmailService {
  private transporter: nodemailer.Transporter;

  constructor(private configService: ConfigService) {
    // Cấu hình SMTP transporter
    const smtpHost = this.configService.get<string>('SMTP_HOST', 'smtp.gmail.com');
    const smtpPortStr = this.configService.get<string>('SMTP_PORT', '587');
    const smtpPort = parseInt(smtpPortStr, 10) || 587; // Convert string to number
    const smtpUser = this.configService.get<string>('SMTP_USER');
    const smtpPass = this.configService.get<string>('SMTP_PASS');
    const smtpSecure = this.configService.get<string>('SMTP_SECURE', 'false') === 'true';

    console.log('SMTP Configuration:', {
      host: smtpHost,
      port: smtpPort,
      portType: typeof smtpPort,
      user: smtpUser,
      pass: smtpPass ? '***' : 'MISSING',
      secure: smtpSecure,
    });

    this.transporter = nodemailer.createTransport({
      host: smtpHost,
      port: smtpPort,
      secure: smtpSecure, // true for 465, false for other ports
      auth: {
        user: smtpUser,
        pass: smtpPass,
      },
    });
  }

  async sendCompletionEmail(
    clinicEmail: string,
    clinicName: string,
    orderCode: string,
    completionTime: Date,
    employeeName?: string,
    imageUrls: string[] = [], // Thêm tham số ảnh
  ): Promise<{ success: boolean; message: string }> {
    try {
      const smtpFrom = this.configService.get<string>('SMTP_FROM') || this.configService.get<string>('SMTP_USER');

      // Tạo HTML hiển thị ảnh inline
      const imageHtml = imageUrls.map((url, index) => {
        const cid = `image${index}@phacogen.com`;
        return `
          <div style="margin: 10px 0;">
            <img src="cid:${cid}" alt="Ảnh ${index + 1}" style="max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);" />
            <p style="margin: 5px 0 0 0; font-size: 12px; color: #666; text-align: center;">Ảnh ${index + 1}</p>
          </div>
        `;
      }).join('');

      // Tạo attachments từ image URLs - convert sang đường dẫn file local
      const attachments = imageUrls.map((url, index) => {
        // Convert URL path sang file path: /uploads/... -> ./uploads/...
        const filePath = url.startsWith('/') ? `.${url}` : url;
        return {
          filename: `anh-hoan-thanh-${index + 1}.jpg`,
          path: filePath, // Sử dụng path cho file local
          cid: `image${index}@phacogen.com`,
        };
      });

      const mailOptions = {
        from: smtpFrom,
        to: clinicEmail,
        subject: `Phacogen - Thông báo nhận mẫu - ${orderCode}`,
        html: `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #f9f9f9;">
            <div style="background-color: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
              
              <!-- Header với logo hoặc tên công ty -->
              <div style="text-align: center; margin-bottom: 30px;">
                <h1 style="color: #1976d2; margin: 0; font-size: 24px;">PHACOGEN</h1>
                <p style="color: #666; margin: 5px 0 0 0; font-size: 14px;">Công ty Cổ phần Y Dược Phacogen</p>
              </div>
              
              <!-- Nội dung chính -->
              <div style="background-color: #e3f2fd; padding: 20px; border-radius: 8px; border-left: 4px solid #1976d2; margin-bottom: 25px;">
                <p style="margin: 0; font-size: 16px; line-height: 1.6; color: #333;">
                  📢 <strong>Phacogen</strong> xin thông báo nhân viên <strong style="color: #1976d2;">${employeeName || 'N/A'}</strong> 
                  đã nhận mẫu của phòng khám <strong style="color: #1976d2;">${clinicName}</strong>
                </p>
              </div>
              
              <!-- Thông tin chi tiết -->
              <div style="background-color: #f5f5f5; padding: 20px; border-radius: 8px; margin-bottom: 25px;">
                <p style="margin: 0 0 10px 0; font-size: 15px; color: #333;">
                  📅 <strong>Thời gian nhận mẫu:</strong> ${new Date(completionTime).toLocaleString('vi-VN', {
          year: 'numeric',
          month: '2-digit',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit',
          hour12: false
        })}
                </p>
                <p style="margin: 0; font-size: 15px; color: #333;">
                  📋 <strong>Mã lệnh:</strong> ${orderCode}
                </p>
              </div>
              
              <!-- Hình ảnh hoàn thành -->
              ${imageUrls.length > 0 ? `
              <div style="background-color: #f5f5f5; padding: 20px; border-radius: 8px; margin-bottom: 25px;">
                <h3 style="margin: 0 0 15px 0; color: #333; font-size: 16px;">📷 Hình ảnh hoàn thành</h3>
                ${imageHtml}
              </div>
              ` : ''}
              
              <!-- Thông báo bổ sung -->
              <div style="background-color: #fff3e0; padding: 15px; border-radius: 8px; border-left: 4px solid #ff9800; margin-bottom: 25px;">
                <p style="margin: 0; font-size: 14px; color: #666; line-height: 1.6;">
                  ℹ️ Danh sách nhận mẫu sẽ được gửi lại vào cuối ngày
                </p>
              </div>
              
              <!-- Lời cảm ơn -->
              <div style="text-align: center; padding: 20px 0; border-top: 2px solid #e0e0e0;">
                <p style="margin: 0; font-size: 15px; color: #333; line-height: 1.6;">
                  Xin chân thành cảm ơn Quý khách hàng đã tin tưởng<br/>
                  và sử dụng dịch vụ của công ty chúng tôi.
                </p>
              </div>
              
              <!-- Footer -->
              <div style="text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #e0e0e0;">
                <p style="margin: 0 0 5px 0; font-size: 14px; color: #1976d2; font-weight: bold;">
                  Công ty Cổ phần Y Dược Phacogen
                </p>
                <p style="margin: 0; font-size: 12px; color: #666; line-height: 1.5;">
                  Hotline: 086 200 3200	 | Email: ptmkq@phacogen.com<br/>
                  Website: www.phacogen.com
                </p>
              </div>
              
            </div>
            
            <!-- Disclaimer -->
            <div style="text-align: center; margin-top: 20px;">
              <p style="font-size: 11px; color: #999; margin: 0;">
                Email này được gửi tự động từ hệ thống. Vui lòng không trả lời email này.<br/>
                Nếu có thắc mắc, vui lòng liên hệ hotline hoặc email hỗ trợ.
              </p>
            </div>
          </div>
        `,
        attachments, // Đính kèm ảnh
      };

      await this.transporter.sendMail(mailOptions);
      console.log(`Email sent successfully to ${clinicEmail} for order ${orderCode}`);
      return {
        success: true,
        message: `Đã gửi email thông báo đến ${clinicEmail}`,
      };
    } catch (error) {
      console.error('Error sending email:', error);
      return {
        success: false,
        message: `Không thể gửi email: ${error.message || 'Lỗi không xác định'}`,
      };
    }
  }

  async sendBusStationCompletionEmail(
    clinicEmail: string,
    clinicName: string,
    orderCode: string,
    busStationName: string,
    busStationAddress: string,
    soTienCuocNhanMau: number,
    soTienShip: number,
    soTienGuiXe: number,
    imageUrls: string[],
    completionTime: Date,
    employeeName: string
  ): Promise<{ success: boolean; message: string }> {
    try {
      const smtpFrom = this.configService.get<string>('SMTP_FROM') ||
        this.configService.get<string>('SMTP_USER');

      // Tạo HTML hiển thị ảnh inline
      const imageHtml = imageUrls.map((url, index) => {
        const cid = `image${index}@phacogen.com`;
        return `
          <div style="margin: 10px 0;">
            <img src="cid:${cid}" alt="Ảnh ${index + 1}" style="max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);" />
            <p style="margin: 5px 0 0 0; font-size: 12px; color: #666; text-align: center;">Ảnh ${index + 1}</p>
          </div>
        `;
      }).join('');

      // Tạo attachments từ image URLs - convert sang đường dẫn file local
      const attachments = imageUrls.map((url, index) => {
        // Convert URL path sang file path: /uploads/... -> ./uploads/...
        const filePath = url.startsWith('/') ? `.${url}` : url;
        return {
          filename: `anh-hoan-thanh-${index + 1}.jpg`,
          path: filePath, // Đường dẫn file local
          cid: `image${index}@phacogen.com`,
        };
      });

      const mailOptions = {
        from: smtpFrom,
        to: clinicEmail,
        subject: `Phacogen - Thông báo nhận mẫu từ nhà xe - ${orderCode}`,
        html: `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #f9f9f9;">
            <div style="background-color: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
              
              <!-- Header -->
              <div style="text-align: center; margin-bottom: 30px;">
                <h1 style="color: #1976d2; margin: 0; font-size: 24px;">PHACOGEN</h1>
                <p style="color: #666; margin: 5px 0 0 0; font-size: 14px;">Công ty Cổ phần Y Dược Phacogen</p>
              </div>
              
              <!-- Thông báo chính -->
              <div style="background-color: #e3f2fd; padding: 20px; border-radius: 8px; border-left: 4px solid #1976d2; margin-bottom: 25px;">
                <p style="margin: 0; font-size: 16px; line-height: 1.6; color: #333;">
                  📢 <strong>Phacogen</strong> xin thông báo nhân viên <strong style="color: #1976d2;">${employeeName}</strong> 
                  đã nhận mẫu của phòng khám <strong style="color: #1976d2;">${clinicName}</strong> tại nhà xe
                </p>
              </div>
              
              <!-- Thông tin nhà xe -->
              <div style="background-color: #fff3e0; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                <h3 style="margin: 0 0 15px 0; color: #ff9800; font-size: 16px;">🚌 Thông tin nhà xe</h3>
                <p style="margin: 0 0 8px 0; font-size: 14px; color: #333;">
                  <strong>Tên nhà xe:</strong> ${busStationName}
                </p>
                <p style="margin: 0; font-size: 14px; color: #333;">
                  <strong>Địa chỉ:</strong> ${busStationAddress}
                </p>
              </div>
              
              <!-- Thông tin lệnh -->
              <div style="background-color: #f5f5f5; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                <h3 style="margin: 0 0 15px 0; color: #333; font-size: 16px;">📋 Thông tin lệnh</h3>
                <p style="margin: 0 0 8px 0; font-size: 14px; color: #333;">
                  <strong>Mã lệnh:</strong> ${orderCode}
                </p>
                <p style="margin: 0; font-size: 14px; color: #333;">
                  <strong>Thời gian nhận mẫu:</strong> ${new Date(completionTime).toLocaleString('vi-VN', {
          year: 'numeric',
          month: '2-digit',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit',
          hour12: false
        })}
                </p>
              </div>
              
              <!-- Hình ảnh hoàn thành -->
              ${imageUrls.length > 0 ? `
              <div style="background-color: #f5f5f5; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                <h3 style="margin: 0 0 15px 0; color: #333; font-size: 16px;">📷 Hình ảnh hoàn thành</h3>
                ${imageHtml}
              </div>
              ` : ''}
              
              <!-- Lời cảm ơn -->
              <div style="text-align: center; padding: 20px 0; border-top: 2px solid #e0e0e0;">
                <p style="margin: 0; font-size: 15px; color: #333; line-height: 1.6;">
                  Xin chân thành cảm ơn Quý khách hàng đã tin tưởng<br/>
                  và sử dụng dịch vụ của công ty chúng tôi.
                </p>
              </div>
              
              <!-- Footer -->
              <div style="text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #e0e0e0;">
                <p style="margin: 0 0 5px 0; font-size: 14px; color: #1976d2; font-weight: bold;">
                  Công ty Cổ phần Y Dược Phacogen
                </p>
                <p style="margin: 0; font-size: 12px; color: #666; line-height: 1.5;">
                  Hotline: 086 200 3200	 | Email: ptmkq@phacogen.com<br/>
                  Website: www.phacogen.com
                </p>
              </div>
              
            </div>
            
            <!-- Disclaimer -->
            <div style="text-align: center; margin-top: 20px;">
              <p style="font-size: 11px; color: #999; margin: 0;">
                Email này được gửi tự động từ hệ thống. Vui lòng không trả lời email này.<br/>
                Nếu có thắc mắc, vui lòng liên hệ hotline hoặc email hỗ trợ.
              </p>
            </div>
          </div>
        `,
        attachments, // Đính kèm ảnh
      };

      await this.transporter.sendMail(mailOptions);
      console.log(`Bus station email sent successfully to ${clinicEmail} for order ${orderCode}`);
      return {
        success: true,
        message: `Đã gửi email thông báo đến ${clinicEmail}`,
      };
    } catch (error) {
      console.error('Error sending bus station email:', error);
      return {
        success: false,
        message: `Không thể gửi email: ${error.message || 'Lỗi không xác định'}`,
      };
    }
  }
}
