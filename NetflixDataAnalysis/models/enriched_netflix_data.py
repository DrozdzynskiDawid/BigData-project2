from dataclasses import dataclass, asdict
import json

@dataclass
class EnrichedNetflixData:
    date: str
    film_id: int
    user_id: int
    rate: int
    title: str

    def to_json(self) -> str:
        return json.dumps(asdict(self))
