import json
from dataclasses import dataclass, asdict

@dataclass
class NetflixEtlData:
    id: int
    title: str
    month: str
    count_rate: int
    sum_rate: float
    unique_users: int


    def to_json(self) -> str:
        return json.dumps(asdict(self))
