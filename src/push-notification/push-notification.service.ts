import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectModel } from '@nestjs/mongoose';
import { Model, Types } from 'mongoose';
import * as OneSignal from 'onesignal-node';
import { RegisterDeviceDto } from './dto/register-device.dto';
import { UpdatePreferencesDto } from './dto/update-preferences.dto';
import { NotificationPreference } from './schemas/notification-preference.schema';
import { UserDevice } from './schemas/user-device.schema';

@Injectable()
export class PushNotificationService {
  private readonly logger = new Logger(PushNotificationService.name);
  private oneSignalClient: any;

  constructor(
    @InjectModel(UserDevice.name)
    private userDeviceModel: Model<UserDevice>,
    @InjectModel(NotificationPreference.name)
    private notificationPreferenceModel: Model<NotificationPreference>,
    private configService: ConfigService,
  ) {
    const appId = this.configService.get<string>('ONESIGNAL_APP_ID');
    console.log('ONESIGNAL_APP_ID:', appId);
    const apiKey = this.configService.get<string>('ONESIGNAL_REST_API_KEY');

    if (appId && apiKey) {
      this.oneSignalClient = new OneSignal.Client(appId, apiKey);
      this.logger.log('OneSignal client initialized');
    } else {
      this.logger.warn('OneSignal credentials not configured');
    }
  }

  async registerDevice(userId: string, dto: RegisterDeviceDto): Promise<UserDevice> {
    try {
      const existingDevice = await this.userDeviceModel.findOne({
        userId: new Types.ObjectId(userId),
        playerId: dto.playerId,
      });

      if (existingDevice) {
        existingDevice.platform = dto.platform;
        existingDevice.deviceModel = dto.deviceModel;
        existingDevice.appVersion = dto.appVersion;
        existingDevice.osVersion = dto.osVersion;
        existingDevice.isActive = true;
        existingDevice.lastUsedAt = new Date();
        await existingDevice.save();
        this.logger.log(`Device updated for user ${userId}`);
        return existingDevice;
      }

      const device = new this.userDeviceModel({
        userId: new Types.ObjectId(userId),
        playerId: dto.playerId,
        platform: dto.platform,
        deviceModel: dto.deviceModel,
        appVersion: dto.appVersion,
        osVersion: dto.osVersion,
        isActive: true,
        lastUsedAt: new Date(),
      });

      await device.save();
      this.logger.log(`Device registered for user ${userId}`);
      return device;
    } catch (error) {
      this.logger.error(`Failed to register device for user ${userId}`, error);
      throw error;
    }
  }

  async unregisterDevice(userId: string, playerId: string): Promise<void> {
    try {
      const result = await this.userDeviceModel.updateOne(
        {
          userId: new Types.ObjectId(userId),
          playerId,
        },
        { isActive: false },
      );

      if (result.matchedCount === 0) {
        throw new NotFoundException('Device not found');
      }

      this.logger.log(`Device unregistered for user ${userId}`);
    } catch (error) {
      this.logger.error(`Failed to unregister device for user ${userId}`, error);
      throw error;
    }
  }

  async getUserDevices(userId: string): Promise<UserDevice[]> {
    return this.userDeviceModel.find({
      userId: new Types.ObjectId(userId),
      isActive: true,
    });
  }

  async getPreferences(userId: string): Promise<NotificationPreference> {
    let preferences = await this.notificationPreferenceModel.findOne({
      userId: new Types.ObjectId(userId),
    });

    if (!preferences) {
      preferences = new this.notificationPreferenceModel({
        userId: new Types.ObjectId(userId),
        pushEnabled: true,
        soundEnabled: true,
        badgeEnabled: true,
        enabledTypes: {},
      });
      await preferences.save();
    }

    return preferences;
  }

  async updatePreferences(
    userId: string,
    dto: UpdatePreferencesDto,
  ): Promise<NotificationPreference> {
    let preferences = await this.notificationPreferenceModel.findOne({
      userId: new Types.ObjectId(userId),
    });

    if (!preferences) {
      preferences = new this.notificationPreferenceModel({
        userId: new Types.ObjectId(userId),
      });
    }

    if (dto.pushEnabled !== undefined) {
      preferences.pushEnabled = dto.pushEnabled;
    }
    if (dto.soundEnabled !== undefined) {
      preferences.soundEnabled = dto.soundEnabled;
    }
    if (dto.badgeEnabled !== undefined) {
      preferences.badgeEnabled = dto.badgeEnabled;
    }
    if (dto.enabledTypes) {
      preferences.enabledTypes = new Map(Object.entries(dto.enabledTypes));
    }

    await preferences.save();
    this.logger.log(`Preferences updated for user ${userId}`);
    return preferences;
  }

  async sendToUser(
    userId: string,
    title: string,
    message: string,
    data?: any,
  ): Promise<void> {
    try {
      this.logger.log(`[DEBUG] sendToUser called with userId: ${userId}`);
      
      if (!this.oneSignalClient) {
        this.logger.warn('OneSignal not configured, skipping push notification');
        return;
      }

      const preferences = await this.getPreferences(userId);
      if (!preferences.pushEnabled) {
        this.logger.debug(`Push notifications disabled for user ${userId}`);
        return;
      }

      const devices = await this.getUserDevices(userId);
      this.logger.log(`[DEBUG] Found ${devices.length} devices for user ${userId}`);
      if (devices.length > 0) {
        this.logger.log(`[DEBUG] Device details: ${JSON.stringify(devices.map(d => ({ playerId: d.playerId, userId: d.userId.toString(), isActive: d.isActive })))}`);
      }
      
      if (devices.length === 0) {
        this.logger.debug(`No active devices for user ${userId}`);
        return;
      }

      const playerIds = devices.map((d) => d.playerId);

      const notification = {
        contents: { en: message },
        headings: { en: title },
        include_player_ids: playerIds,
        data: data || {},
      };

      const response = await this.oneSignalClient.createNotification(notification);
      this.logger.log(`Push notification sent to user ${userId}, recipients: ${response.body.recipients}`);
      this.logger.debug(`OneSignal response: ${JSON.stringify(response.body)}`);
    } catch (error) {
      this.logger.error(`Failed to send push notification to user ${userId}`, error);
    }
  }

  async sendToMultipleUsers(
    userIds: string[],
    title: string,
    message: string,
    data?: any,
  ): Promise<void> {
    for (const userId of userIds) {
      await this.sendToUser(userId, title, message, data);
    }
  }

  async sendTestNotification(
    title: string,
    message: string,
    options?: {
      userId?: string;
      playerId?: string;
      sendToAll?: boolean;
    },
  ): Promise<any> {
    try {
      if (!this.oneSignalClient) {
        throw new Error('OneSignal not configured');
      }

      let notification: any = {
        contents: { en: message },
        headings: { en: title },
        data: { test: true },
      };

      if (options?.sendToAll) {
        // Gửi đến tất cả users
        notification.included_segments = ['All'];
      } else if (options?.playerId) {
        // Gửi đến một player ID cụ thể
        notification.include_player_ids = [options.playerId];
      } else if (options?.userId) {
        // Gửi đến tất cả devices của user
        const devices = await this.getUserDevices(options.userId);
        if (devices.length === 0) {
          throw new Error('No active devices found for user');
        }
        notification.include_player_ids = devices.map((d) => d.playerId);
      } else {
        throw new Error('Must specify userId, playerId, or sendToAll');
      }

      const response = await this.oneSignalClient.createNotification(notification);
      this.logger.log(`Test notification sent: ${JSON.stringify(response.body)}`);

      return {
        success: true,
        recipients: response.body.recipients,
        id: response.body.id,
      };
    } catch (error) {
      this.logger.error('Failed to send test notification', error);
      throw error;
    }
  }

  async sendToAllUsers(title: string, message: string, data?: any): Promise<any> {
    try {
      if (!this.oneSignalClient) {
        throw new Error('OneSignal not configured');
      }

      const notification = {
        contents: { en: message },
        headings: { en: title },
        included_segments: ['All'],
        data: data || {},
      };

      const response = await this.oneSignalClient.createNotification(notification);
      this.logger.log(`Broadcast notification sent to all users: ${response.body.recipients} recipients`);

      return {
        success: true,
        recipients: response.body.recipients,
        id: response.body.id,
      };
    } catch (error) {
      this.logger.error('Failed to send broadcast notification', error);
      throw error;
    }
  }
}
