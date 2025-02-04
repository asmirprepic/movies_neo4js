# Insert Movies & Set Title
def insert_movies_bulk(tx, data):
    query = """
    UNWIND $data AS row
    MERGE (m:Movie {id: row.movie_id})
    SET m.title = row.title
    """
    tx.run(query, data=data)

def create_indexes(driver):
    with driver.session() as session:
        #session.run("CREATE INDEX actor_name_index IF NOT EXISTS FOR (a:Actor) ON (a.name);")
        #session.run("CREATE INDEX movie_id_index IF NOT EXISTS FOR (m:Movie) ON (m.id);")
        session.run("CREATE INDEX genre_index IF NOT EXISTS FOR (g:Genre) ON (a.genre);")
  
# Connect Movies to Writers
def insert_writers_bulk(tx, data):
    query = """
    UNWIND $data AS row
    MERGE (w:Writer {name: row.writer_name})
    MERGE (m:Movie {id: row.movie_id})
    MERGE (w)-[:WROTE]->(m)
    """
    tx.run(query, data=data)

def insert_directors_bulk(tx,data):
  query = """
  UNWIND $data as row
  MERGE (d:Director {name: row.director_name})
  MERGE (m:Movie {id: row.movie_id})
  MERGE (d)-[:DIRECTED]->(m)

  """
  tx.run(query,data = data)

# Connect Movies to Producers
def insert_producers_bulk(tx, data):
    query = """
    UNWIND $data AS row
    MERGE (p:Producer {name: row.producer_name})
    MERGE (m:Movie {id: row.movie_id})
    MERGE (p)-[:PRODUCED]->(m)
    """
    tx.run(query, data=data)

# Connect Movies to Composers
def insert_composers_bulk(tx, data):
    query = """
    UNWIND $data AS row
    MERGE (c:Composer {name: row.composer_name})
    MERGE (m:Movie {id: row.movie_id})
    MERGE (c)-[:COMPOSED]->(m)
    """
    tx.run(query, data=data)

# Connect Movies to Cinematographers
def insert_cinematographers_bulk(tx, data):
    query = """
    UNWIND $data AS row
    MERGE (c:Cinematographer {name: row.cinematographer_name})
    MERGE (m:Movie {id: row.movie_id})
    MERGE (c)-[:SHOT]->(m)
    """
    tx.run(query, data=data)

# Connect Movies to Genres
def insert_genres_bulk(tx, data):
    query = """
    UNWIND $data AS row
    MERGE (g:Genre {name: row.genre})
    MERGE (m:Movie {id: row.movie_id})
    MERGE (m)-[:BELONGS_TO]->(g)
    """
    tx.run(query, data=data)

# Connect Movies to Production Companies
def insert_companies_bulk(tx, data):
    query = """
    UNWIND $data AS row
    
    MERGE (p:ProductionCompany {name: row.company})
    MERGE (m:Movie {id: row.movie_id})
    MERGE (p)-[:PRODUCED_BY]->(m)
    """
    tx.run(query, data=data)

# Insert Movie Popularity
def insert_movie_popularity_bulk(tx, data):
    query = """
    UNWIND $data AS row
    MERGE (m:Movie {id: row.movie_id})
    SET m.popularity = row.popularity, m.vote_average = row.vote_avg, m.vote_count = row.vote_count
    """
    tx.run(query, data=data)

# Connect Movies to Time Nodes
def insert_time_bulk(tx, data):
    query = """
    UNWIND $data AS row
    MERGE (y:Year {value: row.release_year})
    MERGE (mth:Month {value: row.release_month})
    MERGE (d:Decade {value: row.release_decade})
    MERGE (m:Movie {id: row.movie_id})
    MERGE (m)-[:RELEASED_IN]->(y)
    MERGE (m)-[:RELEASED_IN]->(mth)
    MERGE (m)-[:RELEASED_IN]->(d)
    """
    tx.run(query, data=data)

def insert_actors_bulk(tx, data):
    query = """
    
        UNWIND $data AS row
        MERGE (a:Actor {name: row.actor_name})  // Faster with index!
        MERGE (m:Movie {id: row.movie_id})
        MERGE (a)-[:ACTED_IN]->(m)
    
    """
    tx.run(query,data=data)
    

def insert_actors_bulk_wo_rel(driver, actor_data):
    query = """
    CALL {
        WITH $actor_data AS batch
        UNWIND batch AS row
        MERGE (a:Actor {name: row.actor_name})
    } IN TRANSACTIONS OF 10000 ROWS;
    """
    with driver.session() as session:
        session.run(query, actor_data=actor_data)


def insert_coactors_bulk(tx, data):
    query = """
    UNWIND $data AS row
    MATCH (a1:Actor {name: row.actor1})
    MATCH (a2:Actor {name: row.actor2})
    WHERE a1 <> a2
    MERGE (a1)-[:COACTED_WITH]->(a2)
    """
    tx.run(query, data=data)

def insert_director_actor_relationships(tx, data):
    query = """
    UNWIND $data AS row
    MATCH (d:Director {name: row.director_name})
    MATCH (a:Actor {name: row.actor_name})
    MERGE (d)-[:WORKED_WITH]->(a)
    """
    tx.run(query, data=data)

def insert_actor_genre_relationship(tx, data):
    query = """
    UNWIND $data AS row
    MATCH (a:Actor {name: row.actor_name})
    MATCH (g:Genre {name: row.genre})
    MERGE (a)-[:FREQUENTLY_ACTS_IN]->(g)
    """
    tx.run(query, data=data)

def insert_similar_popularity_movies(tx, data):
    query = """
    UNWIND $data AS row
    MATCH (m1:Movie {id: row.movie1})
    MATCH (m2:Movie {id: row.movie2})
    WHERE ABS(m1.popularity - m2.popularity) < 2.0  # Popularity difference less than 2
    MERGE (m1)-[:SIMILAR_POPULARITY]->(m2)
    """
    tx.run(query, data=data)


