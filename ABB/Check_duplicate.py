import csv

# Function to check duplicates in Address column and track their positions
def check_duplicates_with_positions(file_path):
    # Open and read the CSV file
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        
        # Create a dictionary to store addresses and associated tags along with their row positions
        address_dict = {}
        
        # Store the duplicates along with positions
        duplicates = []
        
        for position, row in enumerate(reader, start=2):  # Start from position 1
            address = row['Address']
            tag = row['Tags']
            
            # Check if the address is already in the dictionary
            if address in address_dict:
                # If the address is found, it means it's a duplicate, add the previous and current positions
                duplicates.append((tag, address, address_dict[address], position))
            else:
                # Otherwise, store the address with its corresponding tag and position
                address_dict[address] = position
        
        # Return the list of duplicates with positions
        return duplicates


# Path to your CSV file
file_path = 'mapping.csv'  # Update with the correct path to your CSV file

# Call the function and get the duplicates with positions
duplicates = check_duplicates_with_positions(file_path)

# Check if any duplicates were found
if duplicates:
    print("Duplicates found:")
    for tag, address, first_position, second_position in duplicates:
        print(f"Tag: {tag}, Address: {address}, First Position: {first_position}, Duplicate Position: {second_position}")
else:
    print("No duplicates found.")
