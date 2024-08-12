# Data Weather

- count: Count of returned observations.

- data: [

  * lat: Latitude (Degrees).

lon: Longitude (Degrees).

sunrise: Sunrise time UTC (HH:MM).

sunset: Sunset time UTC (HH:MM).

timezone: Local IANA Timezone.

station: [DEPRECATED] Nearest reporting station ID.

sources: List of data sources used in response.

ob_time: Last observation time (YYYY-MM-DD HH:MM).

datetime: [DEPRECATED] Current cycle hour (YYYY-MM-DD:HH).

ts: Last observation time (Unix timestamp).

city_name: City name.

country_code: Country abbreviation.

state_code: State abbreviation/code.

pres: Pressure (mb).

slp: Sea level pressure (mb).

wind_spd: Wind speed (Default m/s).

gust: Wind gust speed (Default m/s).

wind_dir: Wind direction (degrees).

wind_cdir: Abbreviated wind direction.

wind_cdir_full: Verbal wind direction.

temp: Temperature (default Celsius).

app_temp: Apparent/"Feels Like" temperature (default Celsius).

rh: Relative humidity (%).

dewpt: Dew point (default Celsius).

clouds: Cloud coverage (%).

pod: Part of the day (d = day / n = night).

weather: {

icon:Weather icon code.

code:Weather code.

description: Text weather description.

}

vis: Visibility (default KM).

precip: Liquid equivalent precipitation rate (default mm/hr).

snow: Snowfall (default mm/hr).

uv: UV Index (0-11+).

aqi: Air Quality Index [US - EPA standard 0 - +500]

dhi: Diffuse horizontal solar irradiance (W/m^2) [Clear Sky]

dni: Direct normal solar irradiance (W/m^2) [Clear Sky]

ghi: Global horizontal solar irradiance (W/m^2) [Clear Sky]

solar_rad: Estimated Solar Radiation (W/m^2).

elev_angle: Solar elevation angle (degrees).

h_angle: [DEPRECATED] Solar hour angle (degrees).

]

# Data Weather Alerts

- lat: Latitude (Degrees).

- lon: Longitude (Degrees).

- timezone: Local IANA Timezone.

- city_name: Nearest city name.

- state_code: State abbreviation/code.

- country_code: Country abbreviation.

- alerts: [

title: Brief description of the alert.

description: Detailed description of the alert.

severity: Severity of the weather phenomena - Either "Advisory", "Watch", or "Warning".

effective_utc: UTC time that alert was issued.

effective_local: Local time that alert was issued.

expires_utc: UTC time that alert expires.

expires_local: Local time that alert expires.

onset_utc: UTC time that alert event starts (If available).

onset_local: Local time that alert event starts (If available).

ends_utc: UTC time that alert event ends (If available).

ends_local: Local time that alert event ends (If available).

uri: An HTTP(S) URI that one may refer to for more detailed alert information.

regions: An array of affected regions.

]
