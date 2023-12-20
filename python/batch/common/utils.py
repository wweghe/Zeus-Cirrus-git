import json, os, inspect, tempfile
from typing import Any, List, Dict, Tuple, Optional
from types import SimpleNamespace
from datetime import date
import datetime

from domain.cirrus_object import CirrusObject


def get_certificate_verification_flag() -> Tuple[str, bool]:
    """
    Reads the environment variable SSL_CERT_FILE and returns a value to
    verify the connection.

    Returns:
        the pem file containing the self signed certificate:
            If the environment variable exists and if the file provided for the env var exists
            The pem file is sent as verify value for the requests API.

        False: otherwise

    """
    result : bool = False
    pem_file = os.environ.get("SSL_CERT_FILE")
    if pem_file is not None:
        result = os.path.exists(pem_file)

    return pem_file, result
    

def replace_string_in_json_file(file_path, old_str, new_str):
    """Replaces string in a json file.

    Args:
        file_path (str): string value path to the JSON file.
        old_str (str): string to replace.
        new_str (str): replacement string.
    """
    # Load the JSON data from the file
    with open(file_path, 'r') as f:
        data = json.load(f)

    # Replace any occurrence of old_str with new_str in the JSON data
    updated_data = replace_string(data, old_str, new_str)

    # Write the updated JSON data back to the file
    with open(file_path, 'w') as f:
        json.dump(updated_data, f)


def replace_string(obj, old_str, new_str):
    """Recursively replace any occurrence of old_str with new_str in obj.
    
    Args:
        obj (str): string value of variable to edit, its type is defined in the function.
        old_str (str): string to replace.
        new_str (str): replacement string.
    Returns:
        string: variable with replacement.
    """
    if isinstance(obj, str):
        return obj.replace(old_str, new_str)
    elif isinstance(obj, dict):
        return {replace_string(k, old_str, new_str): replace_string(v, old_str, new_str) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_string(elem, old_str, new_str) for elem in obj]
    else:
        return obj


def get_dir_path(path : Optional[str]):
    path = path or tempfile.gettempdir()
    if (len(str(path)) > 0): 
        path = path.rstrip("/")
    
    return path


def get_attributes(cls: Any, public_members : bool = True) -> dict:
    
    result : dict = {}
    for base_cls in inspect.getmro(type(cls)):
        dummy = dir(type('dummy', (object,), {}))
        members = [item
                for item in inspect.getmembers(base_cls)
                if item[0] not in dummy and item[0] == '__annotations__']
        if (members is not None and len(members) > 0):
            members = members[0][1]
            if public_members:
                members = dict((k, v) for k, v in members.items() if not str(k).startswith("_"))
            # return members[0][1]
            result.update(members)
    
    return result


def convert_dict_to_object(value: Dict[str, Any]) -> SimpleNamespace:
    value_string = json.dumps(value)
    return convert_str_to_object(value_string)


def convert_dict_to_cirrus_object(value: Dict[str, Any]) -> CirrusObject:
    value_string = json.dumps(value)
    return json.loads(value_string, object_hook = lambda d: CirrusObject(**d))


def convert_object_to_dict(value: SimpleNamespace) -> Dict[str, Any]:
    return json.loads(json.dumps(value, default = lambda s: vars(s)))


def convert_str_to_dict(value : Optional[str]) -> Dict[str, Any]:
    return json.loads(str(value)) \
        if value is not None else None


def convert_str_to_object(value : str) -> SimpleNamespace:
    return json.loads(str(value), object_hook = lambda d: SimpleNamespace(**d))


def remove_attribute(instance : SimpleNamespace,
                     attribute_name :str
                    ) -> SimpleNamespace:
    if hasattr(instance, attribute_name):
        delattr(instance, attribute_name)

    return instance


def setattr_if_not_exist(instance : SimpleNamespace, 
                         attribute_name : str, 
                         value : Any
                        ) -> SimpleNamespace:
    
    if (not hasattr(instance, attribute_name)):
        setattr(instance, attribute_name, value)
    return instance


def convert_xlsx_config_value(value: Any, to_type : type) -> Any:

        if (to_type is None or len(str(to_type)) == 0): raise ValueError(f"to_type cannot be empty")

        if to_type == str:
            return str(value)
        
        elif to_type == bool:
            return json.loads(str(value).lower())
        
        elif (to_type in (object, SimpleNamespace, list)):
            if (value is not None and len(str(value)) > 0):
                return json.loads(str(value), object_hook = lambda d: SimpleNamespace(**d))
            else: 
                return None
        
        elif to_type == float:
            return float(value)
            
        elif (to_type == date):
            if isinstance(value, datetime.datetime):
                return str(value.date())
            elif isinstance(value, datetime.date):
                return str(value)
            
        elif (to_type == datetime.datetime):
            if isinstance(value, datetime.datetime):
                return value.isoformat()
            elif isinstance(value, datetime.date):
                return datetime.datetime.combine(
                    value, 
                    datetime.datetime.min.time(), 
                    tzinfo=datetime.timezone.utc) \
                .isoformat()
            
        return value