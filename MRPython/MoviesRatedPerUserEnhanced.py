# input:  input/ratings.csv
# output: number of movies rated per user 

from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesRatedPerUserEnhanced(MRJob):
	def mapper(self, _, line):
		(userId, movieId, rating, timestamp) = line.split(',')
		yield userId, 1 

	def combiner(self, userId, values):
		yield userId, sum(values)

	def reducer(self, userId, values):
		yield userId, sum(values)

if __name__ == '__main__':
	MoviesRatedPerUserEnhanced.run()

