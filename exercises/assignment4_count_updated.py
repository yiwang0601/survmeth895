# open the txt file
# A question here, I got this line from your hint but it seems that this line is useless
# I tried the code without the first line and it still works so do we need to keep this?
file_name = "rj.txt"

# open the txt file
my_file = open( "rj.txt", "r")

# set variable number of lines, words and characters
count_lines = 0
count_words = 0
count_characters = 0


# loop over lines in file.
for line in my_file:

	# Take words out of lines and put it in a new list named words
    words = line.split()

    # add 1 to number of lines when looping over each line
    count_lines += 1

    # Length of words each line is the number of words, add this value to count of words
    count_words += len(words)

    # Length of a line is the number of characters, add this value to count of characters
    count_characters += len(line)

print ( "There are " + str (count_lines ) + " lines in this specific Romeo and Juliet file.")
print ( "There are " + str (count_words ) + " words in this specific Romeo and Juliet file.")
print ( "There are " + str (count_characters ) + " characters in this specific Romeo and Juliet file.")

