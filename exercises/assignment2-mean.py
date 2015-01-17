#Program to find the mean of all numbers stored in a list

#list of numbers
numbers = [3,6,5,4,1,8,2,4,7,11,2,6,3,12,1]

#variable to store the sum
sum = 0

#variable to store the count
count = 0

#loop over the list of numbers
for val in numbers:
	sum = sum+val
	count = count+1

#divide sum by the count to get the mean
mean = sum/count

#print the mean
print("The mean of " + str(numbers) + " is " + str(mean) )

