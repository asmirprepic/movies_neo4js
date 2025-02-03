import pandas as pd
from neo4j_handling.neo4j_connection import Neo4jConnection
from neo4j_handling.neo4j_queries import (insert_writers_bulk, insert_producers_bulk, insert_composers_bulk, insert_cinematographers_bulk, 
                           insert_genres_bulk, insert_companies_bulk, insert_movie_popularity_bulk, insert_time_bulk, insert_movies_bulk,insert_actors_bulk,
                           create_indexes,insert_directors_bulk)
import time

class Neo4jLoader:

  def __init__(self,uri,user,password):
    self.conn = Neo4jConnection(uri,user,password)

  def load_data(self,file_path,insert_function,column_mapping):
    df = pd.read_csv(file_path)
    df = df.dropna(subset=column_mapping)
    total_rows = len(df)
    start_time = time.time()
    data_list = df[column_mapping].to_dict(orient="records")

    if "actors.csv" in file_path or "movies.csv" in file_path or "grenes.csv" in file_path:
      create_indexes(self.conn._driver)

    with self.conn._driver.session() as session:
        for i in range(0, total_rows, 10000):
            batch = df.iloc[i:i + 10000].to_dict(orient="records")
            session.write_transaction(insert_function, batch)  
            print(f"Inserted {min(i + 10000, total_rows)}/{total_rows} rows from {file_path}...")

    end_time = time.time()  
    elapsed_time = end_time - start_time
    print(f"{file_path} fully inserted! Time taken: {elapsed_time:.2f} seconds\n")

  def load_all(self, data_folder):
    #self.load_data(data_folder + "movies.csv", insert_movies_bulk, ['movie_id', 'title'])
    #self.load_data(data_folder + "writers.csv", insert_writers_bulk, ['movie_id', 'writer_name'])
    #self.load_data(data_folder + "producers.csv", insert_producers_bulk, ['movie_id', 'producer_name'])
    #self.load_data(data_folder + "composers.csv", insert_composers_bulk, ['movie_id', 'composer_name'])
    #self.load_data(data_folder + "cinematographers.csv", insert_cinematographers_bulk, ['movie_id', 'cinematographer_name'])
    #self.load_data(data_folder + "actors.csv", insert_actors_bulk, ['movie_id', 'actor_name'])
    #self.load_data(data_folder + "genres.csv", insert_genres_bulk, ['movie_id', 'genre'])
    self.load_data(data_folder + "directors.csv", insert_directors_bulk, ['movie_id', 'director_name'])
    #self.load_data(data_folder + "companies.csv", insert_companies_bulk, ['movie_id', 'company'])
    #self.load_data(data_folder + "popularity.csv", insert_movie_popularity_bulk, ['movie_id', 'popularity'])
    #self.load_data(data_folder + "release.csv", insert_time_bulk, ['movie_id', 'release_year', 'release_month', 'release_decade'])
    

  def close(self):
      self.conn.close()

  
  def load_csv_directly(self, tx, csv_file_name, relationship_query):
      query = f"""
      USING PERIODIC COMMIT
      LOAD CSV WITH HEADERS FROM "file:///{csv_file_name}" AS row
      {relationship_query}
      """
      tx.run(query)

  def load_csv_from_colab(self, colab_ip, csv_filename):
    csv_url = f"http://{colab_ip}:8081/{csv_filename}"
    query = f"""
    CALL {{
        LOAD CSV WITH HEADERS FROM "{csv_url}" AS row
        WITH row
        MERGE (a:Actor {{name: row.actor_name}})
        MERGE (m:Movie {{id: row.movie_id}})
        MERGE (a)-[:ACTED_IN]->(m)
    }} IN TRANSACTIONS OF 10000 ROWS;
    """
    with self.conn._driver.session() as session:
        session.run(query)




  def load_all_csv(self, colab_ip):
    with self.conn._driver.session() as session:
        session.write_transaction(lambda tx: self.load_csv_from_colab(colab_ip, "actors.csv"))


