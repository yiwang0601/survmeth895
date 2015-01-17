# Python program to find the
# H.C.F of two input number

# define a function
def hcf( x, y ):

    """This function takes two
    integers and returns the H.C.F"""

    # choose the smaller number
    if x > y:
    
        smaller = y
    
    else:
    
        smaller = x

    #-- END check to find slmaller number --#

    # loop over integers from 1 to the smaller number.
    for i in range( 1, smaller + 1 ):

        # see if each divided by the other results in no remainder (% = modulo operator - remainder part of a division)
        if ( ( x % i == 0 ) and ( y % i == 0 ) ):

            # this is a common factor.  Store it, but also keep looking,
            #    in case there is a greater one.
            hcf = i

        #-- END check to see if HCF --#

    #-- END loop over integers from 1 to smaller of the two numbers --#

    return hcf

#-- END function hcf() --#

# take input from the user
#num1 = int(input("Enter first number: "))
#num2 = int(input("Enter second number: "))

# OR just enter the numbers you want here
num1 = 36
num2 = 18

print( "The H.C.F. of " + str( num1 ) + " and " + str( num2 ) + " is " + str( hcf( num1, num2 ) ) )