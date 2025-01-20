import uuid
import os

# Function is responsible to check and format bucket names into valid ones

def generate_valid_bucket_name(base_name="my-unique-bucket"):
    """
    Generate a random sufix and valid s3 compatible bucket name from a given base_name. 
    This functions gets rid of any unique "." occurrences.
    :param base_name: str: base_name which must be a string or compatible with string conversion
    :return: str: valid s3 bucket name
    """

    unique_id = uuid.uuid4().hex[:6]  # Short unique suffix

    # assuring base name is a string
    try:
        base_name = ("test-" + str(base_name.replace("_", "-")) + unique_id).lower()
    except Exception as e:
        raise Exception(f"Error converting base_name to string: {e}")

    new_name = []

    for char in base_name:
        if ((char >= 'a' and char <= 'z') or (char >= '0' and char <= '9') or char == '-'):
            new_name.append(char)


    return "".join(new_name)

# Function which will be using to create mock files with different sizes

def create_big_file(file_path, size = {'size': 100, 'unit': 'mb'}):
    """
    Create a big file with the specified size in the specified path
    :param file_path: str: path to the file to be created
    :param size: dict: value containing the an int sie and a stirng unit
    :return: int: size of the file created
    """

    size = 1024 

    units = {
        'kb': 1024,
        'mb': 1024 * 1024,
        'mb': 1024 * 1024 * 1024,
    }

    if size['size'].lower() not in units:
        raise Exception(f"Invalid unit: {size['unit']}")
    
    # Creating a file of size * unit
    size = size * units[size['unit'].lower()]
    with open(file_path, 'wb') as file:
        file.write(b'a' * size)

    # yielding to the calling function
    try:
        yield size
    finally:
        os.remove(file_path)
