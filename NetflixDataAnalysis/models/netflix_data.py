from dataclasses import dataclass

@dataclass
class NetflixData:
    date: str
    film_id: int
    user_id: int
    rate: int