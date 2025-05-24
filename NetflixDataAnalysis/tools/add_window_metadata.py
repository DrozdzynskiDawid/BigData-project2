from typing import Iterable
from pyflink.datastream import ProcessWindowFunction
from NetflixDataAnalysis.models.netflix_data_agg import NetflixDataAgg
from NetflixDataAnalysis.tools.reduce_movie_data import reduce_movie_data


class AddWindowMetadata(ProcessWindowFunction):
    def process(self, key, context, elements: Iterable[NetflixDataAgg]):
        agg_result = None
        for elem in elements:
            if agg_result is None:
                agg_result = elem
            else:
                agg_result = reduce_movie_data(agg_result, elem)

        agg_result.window_start = context.window().start
        agg_result.window_end = context.window().end

        return [agg_result]