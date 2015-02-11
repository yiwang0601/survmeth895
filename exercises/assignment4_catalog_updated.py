# open the txt file
file_name = "rj.txt"
my_file = open("rj.txt", "r")

# create a dictionary
word_counts = {}

# define what is punctuation for later use
punc = set('!@#$%^&*()_-+={}[]:;"\|<>,.?/~`')

# loop over text to capture words
for line in my_file:

	# Take words out of lines and put it in a new list named words
	word_list = line.split()

	# Loop over word list
	for word in word_list:

		# For each word change all characters to lower case
		# to make words in consistant
		word = word_list.lower()

		# Remove all punctuation for consistancy
		word = ''.join((x for x in word if x not in punc))

		# Is the word present in the dictionary?
		if word in word_counts:

			# if yes then add 1 to the frequency of the word
			word_counts[word] = word_counts[word] + 1

		# if the word not present in the dictionary
		else:

			# add the word in the dictionary and set the frequency to be 1
			word_counts[word] = 1

# for each word in the dictionary
for i in word_counts:

	# print the word and its frequency with the word
	print(i, counts[i])



