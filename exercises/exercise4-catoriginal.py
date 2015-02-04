# open the txt file
file_name = "rj.txt"
my_file = open( "rj.txt", "r")

# create a dictionary
counts = {}



# remove punctuation
punc = set('!@#$%^&*()_-+={}[]:;"\|<>,.?/~`')

# loop over text to capture words
for line in my_file:
	words = line.split()
	for word in words:
		word = word.lower()
		word = ''.join((x for x in word if x not in punc))
		if word in counts:
			counts[word] = counts[word] + 1
		else:
			counts[word] = 1

print (counts)
