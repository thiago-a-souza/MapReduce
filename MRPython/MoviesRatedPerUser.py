# input:  input/ratings.csv
# output: number of movies rated per user 

from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesRatedPerUser(MRJob):
	def mapper(self, _, line):
		(userID, movieID, rating, timestamp) = line.split(',')
		yield userID, 1 

	def reducer(self, userID, values):
		yield userID, sum(values)

if __name__ == '__main__':
	MoviesRatedPerUser.run()


