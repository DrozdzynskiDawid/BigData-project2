from dataclasses import dataclass, asdict
import json
from datetime import datetime

@dataclass
class NetflixDataAgg:
    window_start: int
    window_end: int
    title: str
    count: int
    avg_rate: float

    def to_json(self) -> str:
        data = asdict(self)
        data["window_start"] = datetime.fromtimestamp(self.window_start / 1000).isoformat()
        data["window_end"] = datetime.fromtimestamp(self.window_end / 1000).isoformat()
        return json.dumps(data)
