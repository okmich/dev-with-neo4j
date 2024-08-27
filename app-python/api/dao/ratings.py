from api.data import ratings
from api.exceptions.notfound import NotFoundException

from api.data import goodfellas


class RatingDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver=driver

    """
    Add a relationship between a User and Movie with a `rating` property.
    The `rating` parameter should be converted to a Neo4j Integer.
    """
    # tag::add[]
    def add(self, user_id, movie_id, rating):
        # TODO: Create function to save the rating in the database
        def work(tx):
            query = """
                MATCH (u:User {userId: $userId})
                MATCH (m: Movie {tmdbId: $movieId})
                MERGE (u)-[r:RATED]->(m)
                SET r.rating = $rating, r.timestamp=datetime()
                return m {
                    .*,
                    rating: r.rating
                } AS movie
            """
            return tx.run(query, userId=user_id, movieId=movie_id, rating=rating).single()

        # TODO: Call the function within a write transaction
        with self.driver.session() as session:
            record = session.execute_write(lambda tx: work(tx)) 
            if record is None:
                raise NotFoundException()
        # TODO: Return movie details along with a rating
        return record[0]
    # end::add[]


    """
    Return a paginated list of reviews for a Movie.

    Results should be ordered by the `sort` parameter, and in the direction specified
    in the `order` parameter.
    Results should be limited to the number passed as `limit`.
    The `skip` variable should be used to skip a certain number of rows.
    """
    # tag::forMovie[]
    def for_movie(self, id, sort = 'timestamp', order = 'ASC', limit = 6, skip = 0):
        def get_all(tx):
            query = f"""
                MATCH (u:User)-[r:RATED]->(m:Movie {{tmdbId: $id}})
                RETURN r {{
                    .rating,
                    .timestamp,
                    user: u {{
                        .userId, .name
                    }}
                }} AS review
                ORDER BY r.{sort} {order}
                SKIP $skip
                LIMIT $limit
            """
            results = tx.run(query, id=id, skip=skip, limit=limit)
            return [rec.get("review") for rec in results]
        
        with self.driver.session() as session:
            return session.execute_read(get_all)
    # end::forMovie[]
