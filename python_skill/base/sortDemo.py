# Sort a list of numbers in ascending order
numbers = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5]
numbers.sort()
print(numbers)

# Sort a list of numbers in descending order
numbers = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5]
numbers.sort(reverse=True)
print(numbers)

# Sort a list of strings in alphabetical order
words = ['dog', 'cat', 'elephant', 'bird', 'fish']
words.sort()
print(words)

# Sort a list of strings in reverse alphabetical order
words = ['dog', 'cat', 'elephant', 'bird', 'fish']
words.sort(key=lambda x: x[::-1], reverse=True)
print(words)
