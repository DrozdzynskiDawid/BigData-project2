from typing import Iterable

from pyflink.common import Types
from pyflink.datastream import ProcessWindowFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor
from datetime import datetime

from NetflixDataAnalysis.models.enriched_netflix_data import EnrichedNetflixData
from NetflixDataAnalysis.models.netflix_ETL_data import NetflixEtlData


class UniqueUserAggregator(ProcessWindowFunction):

    def open(self, runtime_context: RuntimeContext):
        self.user_state_desc = MapStateDescriptor(
            "user_state",
            Types.INT(),
            Types.BOOLEAN()
        )
        self.user_state = runtime_context.get_map_state(self.user_state_desc)

    def process(self, key, context, elements: Iterable[EnrichedNetflixData]):
        movie_id, movie_title = key
        rating_count = 0
        rating_sum = 0.0

        self.user_state.clear()

        for e in elements:
            user_id = e.user_id
            rating = e.rate
            rating_count += 1
            rating_sum += rating
            self.user_state.put(user_id, True)

        unique_user_count = len(list(self.user_state.keys()))

        window_start = context.window().start
        month_str = datetime.utcfromtimestamp(window_start / 1000).strftime('%Y-%m')

        result = NetflixEtlData(
            id=movie_id,
            title=movie_title,
            month=month_str,
            count_rate=rating_count,
            sum_rate=rating_sum,
            unique_users=unique_user_count
        )

        return [result]
