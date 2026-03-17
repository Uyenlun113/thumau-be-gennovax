import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';

export interface GeocodingResult {
  address: string;
  latitude: number;
  longitude: number;
  formattedAddress: string;
}

@Injectable()
export class GeocodingService {
  private readonly logger = new Logger(GeocodingService.name);
  private readonly apiKey: string;
  private readonly baseUrl = 'https://maps.googleapis.com/maps/api/geocode/json';

  constructor() {
    this.apiKey = process.env.GOOGLE_MAPS_API_KEY || '';
    if (!this.apiKey) {
      this.logger.warn('GOOGLE_MAPS_API_KEY not configured');
    }
  }

  async geocodeAddress(address: string): Promise<GeocodingResult | null> {
    // Try Google Maps first if API key is available
    if (this.apiKey) {
      try {
        const response = await axios.get(this.baseUrl, {
          params: {
            address,
            key: this.apiKey,
            language: 'vi',
            region: 'vn',
          },
        });

        if (response.data.status === 'OK' && response.data.results.length > 0) {
          const result = response.data.results[0];
          return {
            address,
            latitude: result.geometry.location.lat,
            longitude: result.geometry.location.lng,
            formattedAddress: result.formatted_address,
          };
        }

        this.logger.warn(`Google Maps geocoding failed for address: ${address}, status: ${response.data.status}`);
      } catch (error) {
        this.logger.error(`Error with Google Maps geocoding: ${address}`, error);
      }
    }

    // Fallback to OpenStreetMap Nominatim (free)
    this.logger.log('Falling back to OpenStreetMap Nominatim for geocoding');
    return this.geocodeWithNominatim(address);
  }

  private async geocodeWithNominatim(address: string): Promise<GeocodingResult | null> {
    try {
      // Respect Nominatim usage policy: 1 request per second
      await new Promise(resolve => setTimeout(resolve, 1000));

      const response = await axios.get('https://nominatim.openstreetmap.org/search', {
        params: {
          q: address,
          format: 'json',
          limit: 1,
          'accept-language': 'vi',
          countrycodes: 'vn',
        },
        headers: {
          'User-Agent': 'MedicalSupplyApp/1.0',
        },
        timeout: 10000, // 10 second timeout
        validateStatus: (status) => status < 500, // Don't throw on 4xx errors
      });

      if (response.data && response.data.length > 0) {
        const result = response.data[0];
        return {
          address,
          latitude: parseFloat(result.lat),
          longitude: parseFloat(result.lon),
          formattedAddress: result.display_name,
        };
      }

      this.logger.warn(`Nominatim geocoding failed for address: ${address}`);
      return null;
    } catch (error: any) {
      this.logger.error(`Error with Nominatim geocoding: ${address}`, error.message || error);
      return null;
    }
  }

  async reverseGeocode(latitude: number, longitude: number): Promise<string | null> {
    // Try Google Maps first if API key is available
    if (this.apiKey) {
      try {
        const response = await axios.get(this.baseUrl, {
          params: {
            latlng: `${latitude},${longitude}`,
            key: this.apiKey,
            language: 'vi',
            region: 'vn',
          },
        });

        if (response.data.status === 'OK' && response.data.results.length > 0) {
          return response.data.results[0].formatted_address;
        }

        this.logger.warn(`Google Maps reverse geocoding failed for coordinates: ${latitude},${longitude}`);
      } catch (error) {
        this.logger.error(`Error with Google Maps reverse geocoding: ${latitude},${longitude}`, error);
      }
    }

    // Fallback to OpenStreetMap Nominatim (free)
    this.logger.log('Falling back to OpenStreetMap Nominatim for reverse geocoding');
    return this.reverseGeocodeWithNominatim(latitude, longitude);
  }

  private async reverseGeocodeWithNominatim(latitude: number, longitude: number): Promise<string | null> {
    try {
      // Respect Nominatim usage policy: 1 request per second
      await new Promise(resolve => setTimeout(resolve, 1000));

      const response = await axios.get('https://nominatim.openstreetmap.org/reverse', {
        params: {
          lat: latitude,
          lon: longitude,
          format: 'json',
          'accept-language': 'vi',
        },
        headers: {
          'User-Agent': 'MedicalSupplyApp/1.0',
        },
        timeout: 10000, // 10 second timeout
        validateStatus: (status) => status < 500, // Don't throw on 4xx errors
      });

      if (response.data && response.data.display_name) {
        return response.data.display_name;
      }

      this.logger.warn(`Nominatim reverse geocoding failed for coordinates: ${latitude},${longitude}`);
      return null;
    } catch (error: any) {
      this.logger.error(`Error with Nominatim reverse geocoding: ${latitude},${longitude}`, error.message || error);
      return null;
    }
  }

  async searchPlaces(query: string): Promise<any[]> {
    // Try Google Maps first if API key is available
    if (this.apiKey) {
      try {
        const response = await axios.get(this.baseUrl, {
          params: {
            address: query,
            key: this.apiKey,
            language: 'vi',
            region: 'vn',
          },
        });

        if (response.data.status === 'OK') {
          return response.data.results.map((result: any) => ({
            address: result.formatted_address,
            latitude: result.geometry.location.lat,
            longitude: result.geometry.location.lng,
            placeId: result.place_id,
          }));
        }

        this.logger.warn(`Google Maps search failed for query: ${query}`);
      } catch (error) {
        this.logger.error(`Error with Google Maps search: ${query}`, error);
      }
    }

    // Fallback to OpenStreetMap Nominatim (free)
    this.logger.log('Falling back to OpenStreetMap Nominatim for search');
    return this.searchWithNominatim(query);
  }

  private async searchWithNominatim(query: string): Promise<any[]> {
    try {
      // Respect Nominatim usage policy: 1 request per second
      await new Promise(resolve => setTimeout(resolve, 1000));

      const response = await axios.get('https://nominatim.openstreetmap.org/search', {
        params: {
          q: query,
          format: 'json',
          limit: 5,
          'accept-language': 'vi',
          countrycodes: 'vn',
        },
        headers: {
          'User-Agent': 'MedicalSupplyApp/1.0',
        },
        timeout: 10000, // 10 second timeout
        validateStatus: (status) => status < 500, // Don't throw on 4xx errors
      });

      if (response.data && response.data.length > 0) {
        return response.data.map((result: any) => ({
          address: result.display_name,
          latitude: parseFloat(result.lat),
          longitude: parseFloat(result.lon),
          placeId: result.place_id,
        }));
      }

      return [];
    } catch (error: any) {
      this.logger.error(`Error with Nominatim search: ${query}`, error.message || error);
      return [];
    }
  }
}
