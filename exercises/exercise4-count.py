# open the txt file
file_name = "rj.txt"
my_file = open( "rj.txt", "r")

# set variable number of lines, words and characters
count_lines = 0
count_words = 0
count_characters = 0


# loop over lines in file.
for line in my_file:
    words = line.split()
    count_lines += 1
    count_words += len(words)
    count_characters += len(line)

print ( "There are " + str (count_lines ) + " lines in this specific Romeo and Juliet file.")
print ( "There are " + str (count_words ) + " words in this specific Romeo and Juliet file.")
print ( "There are " + str (count_characters ) + " characters in this specific Romeo and Juliet file.")




