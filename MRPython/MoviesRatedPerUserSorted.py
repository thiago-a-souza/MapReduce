# input:  input/ratings.csv
# output: number of movies rated per user sorted by number of movies rated

from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesRatedPerUserSorted(MRJob):
	def steps(self):
		return [ MRStep(mapper=self.mapper_get_users,
			reducer=self.reducer_count_occurrences),
			MRStep(reducer=self.reducer_sorted_output)
		]

	def mapper_get_users(self, _, line):
		(userId, movieId, rating, timestamp) = line.split(',')
		yield userId, 1 

	def reducer_count_occurrences(self, userId, values):
		yield str(sum(values)).zfill(5), userId
	
	def reducer_sorted_output(self, count, userIds):
		for userId in userIds:
			yield userId, count 

if __name__ == '__main__':
	MoviesRatedPerUserSorted.run()


