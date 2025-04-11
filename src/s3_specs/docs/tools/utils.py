import uuid
import os
import pytest
import subprocess
from shlex import quote

# Function is responsible to check and format bucket names into valid ones

def generate_valid_bucket_name(base_name="my-unique-bucket"):
    """
    Generate a random sufix and valid s3 compatible bucket name from a given base_name. 
    This functions gets rid of any unique "." occurrences.
    :param base_name: str: base_name which must be a string or compatible with string conversion
    :return: str: valid s3 bucket name
    """

    unique_id = uuid.uuid4().hex[:64]  # Short unique suffix

    # assuring base name is a string
    try:
        base_name = (str(base_name.replace("_", "-")[:20]) + unique_id).lower()
    except Exception as e:
        raise Exception(f"Error converting base_name to string: {e}")

    new_name = []

    for char in base_name:
        if ((char >= 'a' and char <= 'z') or (char >= '0' and char <= '9') or char == '-'):
            new_name.append(char)

    # assuming max bucket name size is 63
    return "".join(new_name)[:63]



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

    if not os.path.exists('/tmp'):
        os.mkdir('/tmp')


    if not os.path.exists(file_path):
        # Create a file
        with open(file_path, 'wb') as f:
            f.write(os.urandom(total_size))
        f.close()

    return total_size         

@pytest.fixture(scope="module")
def fixture_create_small_file(tmp_path_factory: pytest.TempdirFactory):
    """
    Fixture that creates a temporary file

    Return: Pathlib path: path to the file
    """
    obj_name = 'object_' + uuid.uuid4().hex[:10]
    tmp_path = tmp_path_factory.mktemp("temp")/obj_name
    # Populating file
    with open(tmp_path, "wb") as f:
        f.write((100*"a").encode('utf-8'))

    assert os.path.exists(tmp_path), "Temporary object not created"
    return tmp_path


def execute_subprocess(cmd_command: str, expected_failure:bool = False):
    """
    Execute a shell command as a subprocess and handle errors gracefully.
    
    Args:
        cmd_command (str): The command to execute as a string
        
    Returns:
        subprocess.CompletedProcess: The result object containing:
            - returncode
            - stdout
            - stderr
            
    Raises:
        pytest.fail: If the command fails or any other exception occurs
    """
    try:
        command = cmd_command.split('{')
        json = "".join(['{' + x for x in command[1:]])
        if json == "": # Case there is no json string
            final = command
        else: # Json present
            final = command[0] + f"'{json}'"

        # Run the command and capture output
        result = subprocess.run(
            final,
            shell=True,
            capture_output=True,  # Capture stdout and stderr
            text=True,           # Return output as strings (not bytes)
            check=True           # Raise CalledProcessError if returncode != 0
        )
    except subprocess.CalledProcessError as e:
        # Propagate the error to the query 
        if expected_failure == True:
            return e
        
        # Handle command execution failures
        pytest.fail(
            f"Command failed with exit code {e.returncode}\n"
            f"Command: {cmd_command}\n"
            f"Error: {e.stderr}"
        )
    except Exception as e:
        # Handle any other unexpected errors
        pytest.fail(
            f"Unexpected error: {type(e)}: {e}\n"
            f"Command: {cmd_command}\n"
        )

    # Validate true postives
    return result


