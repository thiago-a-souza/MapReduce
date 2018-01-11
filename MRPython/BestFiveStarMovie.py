# input:  input/ratings.csv
# output: returns the frequency and movieId of the best five star movie

from mrjob.job import MRJob
from mrjob.step import MRStep

class BestFiveStarMovie(MRJob):

	def steps(self):
		return [
			MRStep(mapper=self.mapper_get_ratings, 
			reducer=self.reducer_count_ratings),
			MRStep(reducer=self.reducer_find_max)
		]

	def mapper_get_ratings(self, _, line):
		(userId, movieId, rating, timestamp) = line.split(',')
		if(int(float(rating)) == 5):
			yield movieId, 1 

	def reducer_count_ratings(self, movieId, values):
		yield None, (sum(values), movieId)

	def reducer_find_max(self, _, values):
		yield max(values)

		
if __name__ == '__main__':
    BestFiveStarMovie.run()


