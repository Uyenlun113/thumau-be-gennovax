import { Controller, Get, Query } from '@nestjs/common';
import { GeocodingService } from './geocoding.service';

@Controller('geocoding')
export class GeocodingController {
  constructor(private readonly geocodingService: GeocodingService) {}

  @Get('search')
  async searchAddress(@Query('q') query: string) {
    if (!query) {
      return { results: [] };
    }
    const results = await this.geocodingService.searchPlaces(query);
    return { results };
  }

  @Get('geocode')
  async geocode(@Query('address') address: string) {
    if (!address) {
      return { error: 'Address is required' };
    }
    const result = await this.geocodingService.geocodeAddress(address);
    return result || { error: 'Geocoding failed' };
  }

  @Get('reverse')
  async reverseGeocode(
    @Query('lat') lat: string,
    @Query('lng') lng: string,
  ) {
    const latitude = parseFloat(lat);
    const longitude = parseFloat(lng);

    if (isNaN(latitude) || isNaN(longitude)) {
      return { error: 'Invalid coordinates' };
    }

    const address = await this.geocodingService.reverseGeocode(latitude, longitude);
    return address ? { address } : { error: 'Reverse geocoding failed' };
  }
}
