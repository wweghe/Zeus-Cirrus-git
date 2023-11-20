import requests, os, json, urllib3, base64
from urllib.parse import urlparse
from datetime import datetime
from requests.models import Response
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple, Callable
from requests.sessions import Session

from common.errors import *
import common.constants as constants
import common.utils as utils


class RequestService(object):
    
    __AUTH_HEADERS = {'Accept': 'application/json',
                      'Content-Type': 'application/x-www-form-urlencoded'}
    # __AUTH_TOKEN_URI = "/oauth2/default/v1/token"
    _session : Session = None
    _inside_cluster : bool = True
    _auth_url : str = None
    _refresh_token_url : str = None
    _base_url : str = None
    _user : str = None
    _password : str = None
    _verify : bool = False
    _cert_file_path : str = None
    _token : str = None
    _refresh_token : str = None
    _consul_token : str = None
    _session_details : dict = {}
    _client_auth : Tuple[str, str] = None
    _tenant : str = None
    _start_time : datetime = None
    

    # private members
    def __init__(self, 
                 url : str, 
                 auth_url : Optional[str] = None,
                 user : Optional[str] = None, 
                 password : Optional[str] = None, 
                 token : Optional[str] = None,
                 consul_token : Optional[str] = None,
                 client_auth : Optional[str] = None, 
                 cert_verify : bool = True,
                 cert_file_path : str = None,
                 tenant : Optional[str] = None,
                 disable_insecure_warning : Optional[bool] = False
                ) -> None:
        self._inside_cluster = url is None or len(str(url)) == 0
        app_url = self.__resolve_service_url(service = "sas-risk-cirrus-core") if self._inside_cluster else url 
        # remove trailing slash
        app_url = app_url.rstrip("/")

        if disable_insecure_warning:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self._session = requests.session()

        # get authentication url
        if auth_url is None: # derive full authentication url
            if self._inside_cluster:
                auth_url = f"{self.__resolve_service_url(service = 'SASLogon').rstrip('/')}{constants.URI_GET_TOKEN}"
            else: 
                auth_url = f"{app_url}{constants.URI_GET_TOKEN}"
        
        if not self.__is_absolute(auth_url): # relative path, derive base url
            if self._inside_cluster:
                auth_url = f"{self.__resolve_service_url(service = 'SASLogon').rstrip('/')}{auth_url}"
            else:
                auth_url = f"{app_url}{auth_url}"

        self._base_url = app_url
        self._auth_url = auth_url
        self._refresh_token_url = auth_url
        self._user = user or ''
        self._password = password or ''
        self._verify = cert_verify # certificate verification flag
        self._cert_file_path = cert_file_path
        self._token = token
        self._consul_token = consul_token if consul_token is not None else os.getenv("CONSUL_HTTP_TOKEN", None)
        if (client_auth is not None and len(str(client_auth)) > 0):
            client_id, client_secret = client_auth.split(":")
            self._client_auth = (client_id, client_secret)
        
        self._tenant = tenant
        self._start_time = datetime.now()

        if self._inside_cluster:
            client_id = constants.CLIENT_ID_RISK_CIRRUS_CORE
            client_secret = self.__get_client_id_secret_from_consul(client_id)
            self._client_auth = (client_id, client_secret)

        # Get token
        if self.__is_get_token(user, password, token):
            self._token, self._refresh_token, self._session_details = self.__get_token( 
                user = user,
                password = password,
                client_auth = self._client_auth)
            self._start_time = datetime.now()
        else: # auth token was passed instead of user and password
            # need to refresh it to get the right session details
            self._refresh_token, _ = self.__get_refresh_token_from_token()
            self.refresh_token()

        if self._token is None:
            raise AuthError("Token is not obtained")
        
        self.__set_auth_header(token = self._token)


    def __is_absolute(self, url : str): return bool(urlparse(url).netloc)
    

    def __set_auth_header(self, token : str):
        auth_header = { "Authorization": f"Bearer {token}" }
        self._session.headers.update(auth_header)


    def __get_refresh_token_from_token(self) -> Tuple[str, dict]:

        headers = { "Authorization": f"Bearer {self._token}" }
        headers.update(self.__AUTH_HEADERS)
        payload = "grant_type=user_token" \
                    f"&client_id={self._client_auth[0]}"
        authReturn : Response = self._session.post(
                url = self._auth_url, 
                data = payload, 
                headers = headers,
                verify = self._cert_file_path or self._verify)
        if (not authReturn.ok):
            raise AuthError("Failed to get refresh token.", authReturn.text)

        session_details = authReturn.json()
        refresh_token = session_details.get('refresh_token', None)

        return refresh_token, session_details


    def __get_token(self, 
                    user : str, 
                    password : str, 
                    client_auth : Tuple[str, str]
                   ) -> Tuple[str, str, dict]:
        """
        Obtains the access token by user and password.
        Returns access token and session details
        """
        payload = "grant_type=password" \
                    f"&username={user}" \
                    f"&password={password}"
        authReturn : Response = self._session.post(
                url = self._auth_url, 
                auth = client_auth,
                data = payload, 
                headers = self.__AUTH_HEADERS,
                verify = self._cert_file_path or self._verify)
        if (not authReturn.ok):
            raise AuthError("Failed to get access token.", authReturn.text)

        session_details = authReturn.json()
        token = session_details.get('access_token', None)
        refresh_token = session_details.get('refresh_token', None)

        return token, refresh_token, session_details
    

    def __register_viya_client(self):
        raise NotImplementedError

    
    def __is_get_token(self, user : str, password : str, token : str) -> bool:
        return user and password and token is None
    

    def __is_register_viya_client(self, consul_token : str, client_auth : Tuple[str, str], token : str) -> bool:
        return consul_token and client_auth and token is None

    
    def __context_root_to_k8s_name(self, root : str = None) -> str:
        """
            Dictionary of context name to k8s service name.
            If not found, will return the inputed argument
        """
        switcher = {
            "analyticsGateway": "sas-analytics-gateway",
            "analyticsPipelines": "sas-analytics-pipelines",
            "appRegistry": "sas-app-registry",
            "audit": "sas-audit",
            "authorization": "sas-authorization",
            "businessRules": "sas-business-rules",
            "casAccessManagement": "sas-cas-access-management",
            "casManagement": "sas-cas-management",
            "catalog": "sas-catalog",
            "compute": "sas-compute",
            "configuration": "sas-configuration",
            "credentials": "sas-credentials",
            "dataMining": "sas-data-mining",
            "dataPlans": "sas-data-plans",
            "dataTables": "sas-data-tables",
            "decisions": "sas-decisions",
            "featureFlags":"sas-feature-flags",
            "files" : "sas-files",
            "folders": "sas-folders",
            "identities": "sas-identities",
            "jobExecution": "sas-job-execution",
            "launcher": "sas-launcher",
            "mlPipelineAutomation": "sas-ml-pipeline-automation",
            "reports": "sas-reports",
            "reportOperations": "sas-report-operations",
            "reportTemplates": "sas-report-templates",
            "reportTransforms": "sas-report-transforms",
            "riskCirrusBuilder": "sas-risk-cirrus-builder",
            "riskCirrusCore": "sas-risk-cirrus-core",
            "riskCirrusObjects": "sas-risk-cirrus-objects",
            "riskScenarios": "sas-risk-scenarios",
            "riskPipeline": "sas-risk-pipeline",
            "SASLogon" : "sas-logon-app",
            "SASRiskCirrus": "sas-risk-cirrus-app",
            "transfer": "sas-transfer",
            "visualAnalytics": "sas-visual-analytics",
            "workflow": "sas-workflow",
            "workflowDefinition": "sas-workflow-definition",
            "workflowHistory": "sas-workflow-history",
            "templates": "sas-templates",
            "consul": "sas-consul-server"
        }
        return switcher.get(root, root)


    def __resolve_service_url(self, service = None) -> str:
        """
            returns the service url to use for the given context root.
            sys.exit if inputted context root is not found
        """
        k8s_service = self.__context_root_to_k8s_name(root = service)
        if service == k8s_service:
            RuntimeError(f"Unable to resolve service '{service}' as it is not recognized.")

        k8s_service_upper_converted = k8s_service.upper().replace('-','_')

        k8s_service_port_http_var = f"{k8s_service_upper_converted}_SERVICE_PORT_HTTP"
        if os.environ.get('SAS_URL_SERVICE_SCHEME') is not None \
            and os.environ.get(k8s_service_port_http_var) is not None:
            
            return f"{os.environ.get('SAS_URL_SERVICE_SCHEME')}://{k8s_service}:{os.environ.get(k8s_service_port_http_var)}"
        
        raise RuntimeError(f"Cannot resolve URL. " \
            "The following environment variables are missing: " \
            "SAS_URL_SERVICE_SCHEME and {k8s_service_port_http_var}.")


    def __get_full_url(self, url : str) -> str:
        if self._inside_cluster:
            service_idx = 1 if len(url) > 0 and url[0] == '/' else 0
            service = str.split(url, "/")[service_idx]
            service_url = self.__resolve_service_url(service)

            return f"{service_url}{'/' if service_idx == 0 else ''}{url}"
        
        return f"{self._base_url}{'' if url[0] == '/' else '/'}{url}"
    

    def __convert_to_json_string(self, payload) -> str:
        if (payload is None): return {}

        if payload:
            if isinstance(payload, SimpleNamespace):
                return json.dumps(payload, default = lambda s: vars(s))
            if isinstance(payload, list):
                l = []
                for item in payload:
                    s = self.__convert_to_json_string(item)
                    l.append(json.loads(s))
                return json.dumps(l)
            if (isinstance(payload, dict)):
                return json.dumps(payload)
        
        return payload
    
    
    def __handle_response(self, 
                          response : Response,
                          return_type = SimpleNamespace,
                          return_conversion_func : Callable[[str], Any] = None
                         ) -> Tuple[Any, Any]:
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise RequestError(http_status = e.response.status_code, server_response = str(e.response.text)) from None
        
        if return_conversion_func is not None:
            return return_conversion_func(response.text), response

        if (return_type and return_type in (dict, list)):
            return response.json(), response
        elif (return_type and return_type == SimpleNamespace):
            if response.text is not None and len(str(response.text)) > 0:
                return json.loads(response.text, object_hook = lambda d: SimpleNamespace(**d)), response
            return SimpleNamespace(), response
        elif (return_type):
            if response.text is not None and len(str(response.text)) > 0:
                sn = json.loads(response.text, object_hook = lambda d: SimpleNamespace(**d))
                # more safe approach is to instantiate root level properties and leave SimpleNamespace for nested nested properties
                result = return_type(**sn.__dict__)
                
                return result, response
            
            return return_type(), response
        else:
            return response.text, response
        
    

    def __get_b64_decoded_str(self, base64_string=None):
        """
        Take a base64 string decodes and it returns ir as string.
        Process is conver string to bytes --> pass through decode --> get string from bytes
        Args:
            base64_string:

        Returns: utf8 string decoded.

        """
        base64_bytes = base64_string.encode('utf-8')
        str_bytes = base64.b64decode(base64_bytes)
        return str_bytes.decode('utf-8')


    def __remove_client_id_prefix(self, client_id : str, prefix : str = "sas.") -> str:
        
        if client_id is not None and client_id.startswith(prefix):
            return client_id[len(prefix):]
        return client_id


    def __get_client_id_secret_from_consul(self, client_id = constants.CLIENT_ID_RISK_CIRRUS_CORE):
        """
        Function retrieves client_id's secret as string from consul.

        Args:
            client_id: id of the client. Please ensure you don't pass 'sas.' when using this.
        Returns:
            client secret
        """
        
        consul_http_token = os.environ.get('CONSUL_HTTP_TOKEN')
        base_url = self.__resolve_service_url(service = "consul")
        client_id_no_prefix = self.__remove_client_id_prefix(client_id)

        if consul_http_token is None or len(str(consul_http_token)) == 0:
            raise AuthError(f"Failed to get client id '{client_id_no_prefix}' secret from consul. Reason: environmental variable 'CONSUL_HTTP_TOKEN' is empty.")

        url = f"{base_url}/v1/kv/config/{client_id_no_prefix}/oauth2.client.clientSecret?dc=viya"
        header = {"X-Consul-Token": consul_http_token}
        resp = self._session.get(
            url, 
            headers = header, 
            verify = self._cert_file_path or self._verify
            )
        if not resp.ok:
            raise AuthError(f"Failed to get client id '{client_id_no_prefix}' secret from consul.", resp.text)
        resp_dict = resp.json()
        sec_str_b64 = resp_dict[0].get('Value')

        return self.__get_b64_decoded_str(sec_str_b64)


    def refresh_token(self) -> None:
        """
        The function refreshes token once we pass 80% of expires_in seconds.
        It then re-initialize bearer token and self.start_time
        :return:None
        """
        if self._refresh_token is None or len(str(self._refresh_token)) == 0:
            return

        elapsed_seconds : int = (datetime.now() - self._start_time).seconds
        seconds_to_compare = int(self._session_details.get('expires_in', elapsed_seconds) * 90 / 100)

        if 0 < seconds_to_compare <= elapsed_seconds:
            payload = f"grant_type=refresh_token" \
                f"&refresh_token={self._refresh_token}"
                
            authReturn = self._session.post(
                url = self._refresh_token_url, 
                auth = self._client_auth,
                data = payload, 
                headers = self.__AUTH_HEADERS,
                verify = self._cert_file_path or self._verify)
            if not authReturn.ok:
                raise AuthError("Failed to refresh access token", authReturn.text)

            self._start_time = datetime.now()
            self._session_details = authReturn.json()
            self._token = self._session_details.get('access_token', None)
            self._refresh_token = self._session_details.get('refresh_token', None)

            if self._token is None:
                raise AuthError("Failed to obtain new access token through refresh token.")
            
            self.__set_auth_header(token = self._token)


    def get(self, 
            url,
            headers = {}, 
            params = {},
            return_type = SimpleNamespace,
            return_conversion_func : Callable[[str], Any] = None
           ) -> Any:
        resp = self._session.get(
            url = self.__get_full_url(url),
            headers = headers,
            params = params,
            verify = self._cert_file_path or self._verify
            )

        return self.__handle_response(
            response = resp, 
            return_type = return_type, 
            return_conversion_func = return_conversion_func)


    def post(self,
             url,
             payload = {}, 
             headers = {}, 
             params = {},
             return_type = SimpleNamespace,
             return_conversion_func : Callable[[str], Any] = None
            ):
        resp = self._session.post(
            url = self.__get_full_url(url),
            data = self.__convert_to_json_string(payload),
            headers = headers,
            params = params,
            verify = self._cert_file_path or self._verify
            )
        
        return self.__handle_response(
            response = resp, 
            return_type = return_type,
            return_conversion_func = return_conversion_func)
    
    
    def put(self,
              url,
              payload = {}, 
              headers = {}, 
              params = {},
              return_type = SimpleNamespace,
              return_conversion_func : Callable[[str], Any] = None
            ):
        resp = self._session.put(
            url = self.__get_full_url(url),
            data = self.__convert_to_json_string(payload),
            headers = headers,
            params = params,
            verify = self._cert_file_path or self._verify
            )
        return self.__handle_response(
            response = resp, 
            return_type = return_type,
            return_conversion_func = return_conversion_func)


    def patch(self,
              url,
              payload = {}, 
              headers = {}, 
              params = {},
              return_type = SimpleNamespace,
              return_conversion_func : Callable[[str], Any] = None
             ):
        resp = self._session.patch(
            url = self.__get_full_url(url),
            data = self.__convert_to_json_string(payload),
            headers = headers,
            params = params,
            verify = self._cert_file_path or self._verify
            )
        return self.__handle_response(
            response = resp, 
            return_type = return_type,
            return_conversion_func = return_conversion_func)


