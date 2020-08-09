# luftdaten-netatmo-influx-grafana

Dockerized Grafana dashboard visualizing your station data of the following networks:
- [Luftdaten: Air pollution data](http://deutschland.maps.luftdaten.info/#6/51.165/10.455)
- [Netatmo](https://weathermap.netatmo.com)
- [UBA - Umweltbundesamt air pollution data](https://www.umweltbundesamt.de/daten/luftbelastung/aktuelle-luftdaten)   
- [DWD - Deutscher Wetterdienst](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes)

The data is retrieved by a small Python script and served from a local influxDB.

![screenshot](https://github.com/JonathanSchubert/luftdaten-netatmo-influx-grafana/blob/master/screenshot_luftdaten.png)
![screenshot](https://github.com/JonathanSchubert/luftdaten-netatmo-influx-grafana/blob/master/screenshot_netatmo.png)

## Install and config
1. Install Docker
2. Clone repo
3. Set IDs of your sensors in `./configuration.env`
  - LD_SENSOR_ID  = esp8266-XXXXXX
  - UBA_SENSOR_ID = DEBEXXX
  - ...

## Run
1. Execute `docker-compose up --build`
2. Access Grafana through http://localhost:3000 (admin/test)

## Debug the Python data handler
`docker-compose build; docker-compose run --service-ports datahandler`
