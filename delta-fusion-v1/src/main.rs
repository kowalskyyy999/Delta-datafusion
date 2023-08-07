
use crate::arrow::util::pretty::pretty_format_batches;
use deltalake::*;
use datafusion::prelude::*;

use std::sync::Arc;
use std::time::Instant;

async fn query_engine(ctx: SessionContext, query: &str, print_result: bool) {
    let now = Instant::now();
    let result = ctx.sql(query)
                .await
                .unwrap()
                .collect()
                .await
                .expect("Failed collect the data");
    
    if print_result {
        let result = pretty_format_batches(&result).unwrap().to_string();
        println!("{}", result);
    }

    let duration = now.elapsed();
    println!("Time elapsed to execute the query {:?}", duration);

}

#[tokio::main]
async fn main() {
    let now = Instant::now();

    let ctx = SessionContext::new();
    let spotify_url = "hdfs://127.0.1.1:9000/delta/spotify";
    let version = 0;
    let delta_builder = DeltaTableBuilder::from_uri(spotify_url)
                                    .with_version(version)
                                    .load().await.unwrap();

    ctx.register_table("demo", Arc::new(delta_builder)).unwrap();

    let select_all_query = "select * from demo limit 100";
    // query_engine(ctx.clone(), select_all_query, false).await;

    let count_of_row = "select count(*) from demo";
    // query_engine(ctx.clone(), count_of_row, false).await;

    let best_genre_each_year = "
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
    ";
    query_engine(ctx.clone(), best_genre_each_year, true).await;

    let best_song_in_year = "
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
    ";
    query_engine(ctx.clone(), best_song_in_year, true).await;

    let best_song_of_heavy_metal = "
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
    ";
    query_engine(ctx.clone(), best_song_of_heavy_metal, true).await;

    let best_song_of_pop_genre_in_year = "
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
    ";
    query_engine(ctx.clone(), best_song_of_pop_genre_in_year, true).await;

    let top_10_artist_with_popularity_over80 = "
        SELECT artist_name, COUNT(artist_name) best_artist
        FROM demo
        WHERE popularity >= 80
        GROUP BY artist_name
        ORDER BY best_artist DESC
        LIMIT 10
    ";
    query_engine(ctx.clone(), top_10_artist_with_popularity_over80, true).await;

    let stats_tempo_each_genre = "
        WITH avgTempoGenre(genre, min_tempo, avg_tempo, max_tempo) as (
            select genre, min(tempo), avg(tempo), max(tempo)
            from demo
            where tempo is not null and tempo > 20
            group by genre
            order by genre
        )
        SELECT * FROM avgTempoGenre
    ";
    query_engine(ctx.clone(), stats_tempo_each_genre, true).await;

    let top_10_genre_danceability = "
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
    ";
    query_engine(ctx.clone(), top_10_genre_danceability, true).await;

    let best_song_2023_based_popularity_and_genre = "
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
    ";
    query_engine(ctx.clone(), best_song_2023_based_popularity_and_genre, true).await;
    
    let duration_total = now.elapsed();
    println!("Total elapsed time for processing the main function {:?}", duration_total);

}