import random

word_list = ["Apple","Grapes", "Mango", "Orange", "Strawberry" ]

secret_word = random.choice(word_list)

# Step 1: Define a function called check_guess and pass the guess as a parameter
def check_guess(guess) : 
     # Step 3: Check if the guess is in the word
    guess = guess.lower()
    # Step 3: Check if the guess is in the word
    if guess in secret_word : 
        print(f"Good guess! {guess} is in the word")
    else : 
        print(f"Sorry, {guess} is not in the word. Try again")



#The ask_for_input function.


#Step 1: Define a function called ask_for_input.
def ask_for_input() : 
    # Check if the input is valid
    while True : 
        guess = input("Guess a letter : ")
        if len(guess) == 1 and guess.isalpha() :
            break
        else :
            print("Invalid letter. Please, enter a single alphabetical character.")
            
    #Check if the guess is in the word
    check_guess(guess)

ask_for_input()