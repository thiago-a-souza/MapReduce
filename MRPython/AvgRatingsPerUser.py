# input:  input/ratings.csv
# output: average rate per user 

from mrjob.job import MRJob
from mrjob.step import MRStep

class AvgRatingsPerUser(MRJob):
	def mapper(self, key, line):
		(userId,movieId,rating,timestamp) = line.split(',')
		yield userId, float(rating)

	def reducer(self, userId, ratings):
		count = 0
		n = 0
		for i in ratings:
			count += i
			n += 1
		yield userId, float(count)/float(n)

if __name__ == '__main__':
    AvgRatingsPerUser.run()


