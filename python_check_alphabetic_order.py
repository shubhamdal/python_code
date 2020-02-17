import sys

def check_values(valu,alphabets_list):
    for chrctr in valu:
        if (chrctr in alphabets_list):
            flag=True
        else:
            flag=False
            return flag
    return flag



def main(alphabets_list,valu):  
    first_char=alphabets_list.index(valu[0])
    flag=0
    for chrctr in valu:
        next_char=alphabets_list.index(chrctr)
        if (next_char==first_char or next_char>first_char):
            first_char=next_char
        else:
            flag=1
    if (flag==0):
        print("\n\nThe word matches the (a-z) sequence\n\n")
    else:
        print("\n\nThe word DOES NOT matches the (a-z) sequence\n\n")


if __name__ == "__main__":

    alphabets_list=[chr(x) for x in range(ord('a'), ord('z')+1)]
    # ord() Given a string of length one, return an integer representing the Unicode code point of the character
    # chr() returns a string representing a character whose Unicode code point is an integer.
    print("The program checks if the sequence of characters in input word matches the actual alphabetic order (a-z)")
    while(1):
        valu = input("Enter your Word : ").lower()
        if (len(valu)>0 and check_values(valu,alphabets_list)):
            main(alphabets_list,valu)
            sys.exit(0)
        else:
            print("Please enter your word (only alphabets allowed)")

    
