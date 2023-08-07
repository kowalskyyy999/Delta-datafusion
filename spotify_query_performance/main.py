from pyspark.sql import SparkSession
import time

def query_engine(spark, query, print_result=False):
    start = time.time()
    df = spark.sql(query)
    if print_result:
        print(df.show())
    elapsed = time.time() - start
    print(f"Time elapsed to execute the query {elapsed}")

def main():
    start = time.time()
    spark = SparkSession \
            .builder \
            .appName("spotify_query_performance") \
            .enableHiveSupport() \
            .getOrCreate()
    
    version = 0
    spotify_path = "/delta/spotify"
    
    df = spark.read.format('delta').option('versionAsOf', version).load(spotify_path)
    try:
        df.write.saveAsTable('demo')
    except:
        spark.sql("drop table demo")
        df.write.saveAsTable('demo')

    best_genre_each_year =  """
        WITH avgGenrePopulerYear(year, genre, avg_popularity) as (
                select year, genre, avg(popularity)
                from demo
                where popularity is not null
                group by year, genre),
            maxAvgPopulerYear(year, max_popularity) as (
                select year, max(avg_popularity)
                from avgGenrePopulerYear
                group by year)
        select avgGenrePopulerYear.year, genre, avg_popularity 
        from avgGenrePopulerYear
        join maxAvgPopulerYear
        on avg_popularity = max_popularity
        order by avgGenrePopulerYear.year
    """
    query_engine(spark, best_genre_each_year, True)

    best_song_in_year = """
        WITH bestSonginYear(year, genre, max_popularity) as (
            select year, genre, max(popularity)
            from demo
            where popularity is not null
            group by year, genre
        )
        SELECT demo.year, artist_name, track_name, max_popularity as popularity 
        FROM (
            select year, max(max_popularity) max_popularity from bestSonginYear
            group by year
        ) a JOIN demo
        ON max_popularity = popularity and a.year = demo.year
        ORDER BY demo.year
    """
    query_engine(spark, best_song_in_year, True)

    best_song_of_heavy_metal = """
        WITH bestOfHeavyMetal(genre, max_popularity) as (
            select genre, max(popularity)
            from demo
            where genre = 'heavy-metal'
            group by genre
        )
        SELECT artist_name, track_name, popularity
        FROM demo
        JOIN bestOfHeavyMetal
        ON popularity = max_popularity
        WHERE demo.genre = 'heavy-metal'
    """
    query_engine(spark, best_song_of_heavy_metal, True)

    best_song_of_pop_genre_in_year = """
        WITH bestOfPop(year, genre, max_popularity) as (
            select year, genre, max(popularity)
            from demo
            where genre = 'pop'
            group by year, genre
        )
        SELECT demo.year, artist_name, track_name, popularity
        FROM demo
        JOIN bestOfPop 
        ON popularity = max_popularity and demo.year = bestOfPop.year
        WHERE demo.genre = 'pop'
        ORDER by demo.year
    """
    query_engine(spark, best_song_of_pop_genre_in_year, True)

    top_10_artist_with_popularity_over80 = """
        SELECT artist_name, COUNT(artist_name) best_artist
        FROM demo
        WHERE popularity >= 80
        GROUP BY artist_name
        ORDER BY best_artist DESC
        LIMIT 10
    """
    query_engine(spark, top_10_artist_with_popularity_over80, True)

    stats_tempo_each_genre = """
        WITH avgTempoGenre(genre, min_tempo, avg_tempo, max_tempo) as (
            select genre, min(tempo), avg(tempo), max(tempo)
            from demo
            where tempo is not null and tempo > 20
            group by genre
            order by genre
        )
        SELECT * FROM avgTempoGenre
    """
    query_engine(spark, stats_tempo_each_genre, True)

    top_10_genre_danceability = """
        WITH avgDanceableGenre(genre, avg_danceable_genre) as (
                select genre, avg(dancebility)
                from demo
                where dancebility > 0.2 and dancebility < 1
                group by genre
                order by genre
        ),
            avgDanceableSong(avg_danceable) as (
                select avg(avg_danceable_genre)
                from avgDanceableGenre
        )
        SELECT genre, avg_danceable_genre danceability
        FROM avgDanceableGenre
        WHERE avg_danceable_genre >= 0.557
        ORDER BY avg_danceable_genre DESC
        LIMIT 10
    """
    query_engine(spark, top_10_genre_danceability, True)

    best_song_2023_based_popularity_and_genre = """
        WITH bestGenrein2023(year, genre, max_popularity) as (
            select year, genre, max(popularity)
            from demo
            where year = '2023'
            group by year, genre
        )
        SELECT artist_name, track_name, demo.genre, popularity
        FROM demo
        JOIN bestGenrein2023 a
        ON demo.genre = a.genre and popularity = max_popularity
        WHERE demo.year = '2023'
        ORDER BY demo.genre
    """
    query_engine(spark, best_song_2023_based_popularity_and_genre, True)

    elapsed_total = time.time() - start
    print(f"Total elapsed time for processing the main function {elapsed_total}")

if __name__ == "__main__":
    main()