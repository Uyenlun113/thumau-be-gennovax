import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import { ScheduleModule } from '@nestjs/schedule';
import { AuthModule } from './auth/auth.module';
import { ClinicModule } from './clinic/clinic.module';
import { EmailModule } from './email/email.module';
import { NotificationModule } from './notification/notification.module';
import { OrderModule } from './order/order.module';
import { RoleModule } from './role/role.module';
import { SampleCollectionModule } from './sample-collection/sample-collection.module';
import { SupplyModule } from './supply/supply.module';
import { UserModule } from './user/user.module';
import { WorkContentModule } from './work-content/work-content.module';
import { WorkScheduleModule } from './work-schedule/work-schedule.module';
import { PushNotificationModule } from './push-notification/push-notification.module';
import { BusStationModule } from './bus-station/bus-station.module';
import { GeocodingModule } from './geocoding/geocoding.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '.env',
    }),
    ScheduleModule.forRoot(), // Enable scheduled tasks
    MongooseModule.forRoot(process.env.MONGODB_URI || 'mongodb+srv://uyenptforimex_db_user:Ptuylht090821@cluster0.z6zymcn.mongodb.net/tiembanh?retryWrites=true&w=majority', {
    }),
    AuthModule,
    UserModule,
    RoleModule,
    ClinicModule,
    SampleCollectionModule,
    OrderModule,
    EmailModule,
    WorkScheduleModule,
    WorkContentModule,
    NotificationModule,
    SupplyModule,
    PushNotificationModule,
    BusStationModule,
    GeocodingModule,
  ],
})
export class AppModule { }
