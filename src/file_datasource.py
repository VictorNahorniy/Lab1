from csv import reader
from datetime import datetime

from domain.parking import Parking
from domain.aggregated_data import AggregatedData
from domain.accelerometer import Accelerometer
from domain.gps import Gps

class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.cache_data = {}

    def read(self) -> AggregatedData:
        accelerometer_data = self.cache_data.get("accelerometer")
        gps_data = self.cache_data.get("gps")
        parking_data = self.cache_data.get("parking")

        if None in (accelerometer_data, gps_data, parking_data):
            raise ValueError("Missing data for one or more types")

        try:
            x, y, z = map(int, next(reader(accelerometer_data)))
        except (ValueError, TypeError):
            raise ValueError("Invalid accelerometer data format (must be a sequence of integers)")

        try:
            longitude, latitude = map(float, next(reader(gps_data)))
        except (ValueError, TypeError):
            raise ValueError("Invalid GPS data format (must be a sequence of floats)")

        try:
            empty_count = int(next(reader(parking_data))[0])
        except (ValueError, TypeError, IndexError):
            raise ValueError("Invalid parking data format (must be a single integer)")

        accelerometer_obj = Accelerometer(x=x, y=y, z=z)
        gps_obj = Gps(longitude=longitude, latitude=latitude)
        parking_obj = Parking(empty_count=empty_count, gps=gps_obj)

        return AggregatedData(accelerometer=accelerometer_obj, gps=gps_obj,
                              parking=parking_obj, timestamp=datetime.now())

    def startReading(self, *args, **kwargs):
        self.cache_data["accelerometer"] = open(self.accelerometer_filename, 'r')
        self.cache_data["gps"] = open(self.gps_filename, 'r')
        self.cache_data["parking"] = open(self.parking_filename, 'r')

    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        # This one is redundant for now as the reading is infinite
        pass