import random

#Create a class called Hangman.
class Hangman(): 
    #Define the __init__ method to initialise the attributes of the class,
    def __init__(self, word_list, num_lives=5) : 
        #attributes
        self.word = random.choice(word_list)
        self.word_guessed = ['_' for _ in self.word]
        self.num_letters = len(set(self.word))
        self.num_lives = num_lives
        self.word_list = word_list
        self.list_of_guesses = []

#Define the check_guess method
    def check_guess(self, guess) : 
        #Convert the guesses letter to lower case
        guess = guess.lower()

        if guess in self.word : 
            print(f"Good guess! {guess} is in the word")
            for i, letter in enumerate(self.word) : 
                if letter == guess :
                    self.word_guessed[i] = guess
            print(self.word_guessed)

        else : 
            print(f"Sorry, {guess} is not in the word.") 
            self.num_lives -= 1
            print(f"You have {self.num_lives} lives left.")
                   
            

#Define the ask_for_input method
    def ask_for_input(self) : 
        # Check if the input is valid
        while True : 
            guess = input("Guess a letter : ")

            if len(guess) != 1 or not guess.isalpha() :
                print("Invalid letter. Please, enter a single alphabetical character.")

            elif guess in self.list_of_guesses : 
                print("You already tried that letter!")

            else :
                self.list_of_guesses.append(guess)
                self.check_guess(guess)


word_list = ["apple","grapes", "mango", "orange", "strawberry" ]
hangman_game = Hangman(word_list)       
hangman_game.ask_for_input()
print(hangman_game.word_guessed)    
