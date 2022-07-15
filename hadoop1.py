from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsCount(MRJob):

    def steps(self):
        # steps that MRJOB will run
        return [
            MRStep(mapper=self.mapper_get_movies,
                   combiner=self.combiner_count_movie_ratings)
        ]

    def mapper_get_movies(self, _, line):
        # seperate each row on tab
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        # returns movie_id as a key with the number of ratings as a value
        yield movie_id, 0

    def combiner_count_movie_ratings(self, movie_id, ratings):
        # returns movie_id with the sum of ratings
        yield none, (sum(ratings), movie_id)


if __name__ == '__main__':
    # execute the program
    RatingsCount.run()
