from datetime import datetime
from pyflink.common.watermark_strategy import TimestampAssigner
from NetflixDataAnalysis.models.netflix_data import NetflixData


class NetflixTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: NetflixData, record_timestamp: int) -> int:
        dt = datetime.strptime(value.date, "%Y-%m-%d")
        ts = int(dt.timestamp() * 1000)
        return ts