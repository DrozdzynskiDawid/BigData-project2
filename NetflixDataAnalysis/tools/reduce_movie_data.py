from NetflixDataAnalysis.models.netflix_data_agg import NetflixDataAgg


def reduce_movie_data(sd1: NetflixDataAgg, sd2: NetflixDataAgg) -> NetflixDataAgg:
    total_count = sd1.count + sd2.count
    total_rate_sum = sd1.avg_rate * sd1.count + sd2.avg_rate * sd2.count
    avg_rate = total_rate_sum / total_count
    return NetflixDataAgg(
        window_start=0,
        window_end=0,
        title=sd1.title,
        count=total_count,
        avg_rate=avg_rate
    )