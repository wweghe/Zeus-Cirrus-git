from typing import Optional, Any


class AuthError(Exception):
    def __init__(self, 
                 message : str, 
                 error : Optional[str] = None):
        if (error is not None and len(str(error)) > 0):  
            message += f"\nReason:\n{error}"
        super().__init__(message)
        self.error = error


class ConfigurationPropertyNotFoundError(Exception):

    def __init__(self, property : str):
        message = f"Configuration property {repr(property)} was not found."
        super().__init__(message)


class CirrusObjectNotFoundError(Exception):
    def __init__(self, 
                 object_type : str, 
                 key : Optional[str] = None,
                 id : Optional[str] = None,
                 ssc : Optional[str] = None,
                 linked_object_key : Optional[str] = None,
                 error : Optional[str] = None
                ):
        message = f"{object_type} was not found: " \
            f"key = '{key}' / id = '{id}', ssc = '{ssc}', related_key = '{linked_object_key}'."
        if (error is not None and len(str(error)) > 0):  
            message += f"\n{error}"
        super().__init__(message)
        self.object_type = object_type
        self.key = key
        self.id = id
        self.ssc = ssc
        self.error = error


class RequestError(Exception):
    def __init__(self, 
                 http_status : int,
                 server_response : Optional[str] = None):
        message = f"\nHTTP_STATUS: {http_status}"
        if (server_response is not None and len(str(server_response)) > 0):
            message += f"\nServer response:\n{server_response}"
        super().__init__(message)
        self.server_response = server_response
        self.http_status = http_status


class ScriptExecutionTimeoutError(Exception):
    def __init__(self, error : Optional[str] = None):
        message = f"Timeout reached while waiting for script execution to finish."
        if (error is not None and len(str(error)) > 0): 
            message += f"\n{error}"
        super().__init__(message)
        self.error = error


class ScriptExecutionError(Exception):
    def __init__(self, status : Optional[str] = None, error: Optional[str] = None):
        message = f"Script execution was unsuccessfull (status: {repr(status)})."
        if (error is not None and len(str(error)) > 0): 
            message += f"\n{error}"
        super().__init__(message)
        self.error = error
        self.status = status


class ScriptParameterResolutionError(Exception):
    def __init__(self, parameter_name : str, expression : str, error : Optional[Exception] = None):
        message = f"Failed to resolve script parameter {repr(parameter_name)} expression. "
        if expression is not None: 
            message += f"\nExpression: {repr(expression)}. "
        if (error is not None):
            message += f"\nError: {str(error)}"
        super().__init__(message)
        self.parameter_name = parameter_name
        self.expression = expression


class WorkflowWaitTimeoutError(Exception):
    def __init__(self, error : Optional[str] = None):
        message = f"Timeout reached while waiting for workflow to finish."
        if (error is not None and len(str(error)) > 0): 
            message += f"\n{error}"
        super().__init__(message)
        self.error = error


class ParseError(Exception):
    
    def __init__(self, message : str, sheet_name : Optional[str] = None, row : Optional[int] = None):
        self.message = message
        self.row = row
        self.sheet_name = sheet_name
        super().__init__(message)


class CycleError(Exception):
    
    def __init__(self, message : str):
        super().__init__(message)


class BatchJobStepCancelationError(Exception):

    def __init__(self, *args, **kwargs: Any) -> None:
        message = f"Batch job step canceled by the user."
        super().__init__(message)


class BatchJobCancelationError(Exception):

    def __init__(self, *args, **kwargs: Any) -> None:
        message = f"Batch job canceled by the user."
        super().__init__(message)
