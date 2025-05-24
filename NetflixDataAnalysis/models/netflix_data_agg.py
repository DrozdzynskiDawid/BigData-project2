from dataclasses import dataclass, asdict
import json

@dataclass
class NetflixDataAgg:
    title: str
    count: int
    avg_rate: float

    def to_json(self) -> str:
        return json.dumps(asdict(self))
