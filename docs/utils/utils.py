import uuid
import os
import logging

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



def convert_unit(size = {'size': 100, 'unit': 'mb'}) -> int:
    """
    Converts a dict containing a int and a str into a int representing the size in bytes
    :param size: dict: {'size': int, 'unit': ('kb', 'mb', 'gb')}
    :return: int: value in bytes of size
    """

    units_dict = {
        'kb': 1024,
        'mb': 1024 * 1024,
        'gb': 1024 * 1024 * 1024,
    }
    
    unit = size['unit'].lower()

    # Check if it is a valid unit to be converted
    if unit not in units_dict:
        raise ValueError(f"Invalid unit: {size['unit']}")

    return size['size'] * units_dict.get(unit)



def create_big_file(file_path: str, size={'size': 100, 'unit': 'mb'}) -> int:
    """
    Create a big file with the specified size using a temporary file.
    
    :param size: dict: A dictionary containing an int 'size' and a str 'unit'.
    :yield: str: Path to the temporary file created.
    """

    total_size = convert_unit(size)

    if not os.path.exists('./tmp_files'):
        os.mkdir('./tmp_files')


    if not os.path.exists(file_path):
        # Create a file
        with open(file_path, 'wb') as f:
            f.write(os.urandom(total_size))
        f.close()

    return total_size

    
