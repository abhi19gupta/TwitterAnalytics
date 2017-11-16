import matplotlib.pyplot as plt

def plot_rate_of_field(field_names, window):

	f = open('log.txt')

	counts = []
	rates = []

	total_cnt = 0
	total_t = 0
	cnt = 0
	t = 0

	for line in f:
		if line.startswith('\t'):
			words = line.split(' ')
			for i,word in enumerate(words):
				if word in field_names:
					cnt += int(words[i+1][1:-1])
					t += float(words[i+2])
					if (cnt >= window):
						total_cnt += cnt
						total_t += t
						counts.append(total_cnt)
						rates.append(cnt/t)
						cnt = 0
						t = 0

	print(counts, rates)
	print(total_t)

	plt.plot(counts, rates)
	plt.show()

def plot_tweet_rate(filename):

	cumm_counts = []
	rates = []

	cumm_cnt = 0
	cumm_time = 0

	f = open(filename,'r')
	for line in f:
		if line.startswith('\t'):
			words = line.split(' ')
			if words[2] == 'Tweets':
				cumm_time += float(words[4])
			if len(words) > 5 and words[5] == 'Synced':
				cumm_time += float(words[7])
				curr_cnt = int(words[6][1:-1])
				rates.append(curr_cnt/cumm_time)
				cumm_cnt += curr_cnt
				cumm_counts.append(cumm_cnt)
				cumm_time = 0

	f.close()

	# print(cumm_counts)
	# print(rates)

	plt.plot(cumm_counts, rates)
	plt.show()



# plot_rate_of_field(['Tweets'],1000000)
# plot_rate_of_field(['Favorites'],100000)
# plot_rate_of_field(['Followers','Friends'],500000)

plot_tweet_rate('log_2017-11-15 19:01:03.148204.txt')