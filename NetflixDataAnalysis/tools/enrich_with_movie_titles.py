import csv
from pyflink.datastream.functions import MapFunction, RuntimeContext
from ..models.enriched_netflix_data import EnrichedNetflixData
from ..models.movie_data import MovieData
from ..models.netflix_data import NetflixData

class EnrichWithMovieTitles(MapFunction):
    def __init__(self, movie_titles_file_path: str):
        self.movie_titles_file_path = movie_titles_file_path
        self.movie_map: dict[int, MovieData] = {}

    def open(self, runtime_context: RuntimeContext):
        self.movie_map = self._load_movie_map()

    def map(self, netflix_data: NetflixData) -> EnrichedNetflixData:
        movie_info = self.movie_map.get(netflix_data.film_id)
        if movie_info:
            title = movie_info.title
        else:
            title = "Unknown"

        return EnrichedNetflixData(
            date=netflix_data.date,
            film_id=netflix_data.film_id,
            user_id=netflix_data.user_id,
            rate=netflix_data.rate,
            title=title
        )

    def _load_movie_map(self) -> dict[int, MovieData]:
        movie_map = {}
        with open(self.movie_titles_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            next(reader, None)
            for parts in reader:
                if len(parts) < 3:
                    continue
                try:
                    film_id = int(parts[0])
                    year = int(parts[1]) if parts[1].isdigit() else None
                    title = parts[2].strip()
                    movie_map[film_id] = MovieData(film_id, year, title)
                except Exception:
                    pass
        return movie_map
