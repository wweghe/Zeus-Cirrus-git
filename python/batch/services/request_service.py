import requests, os, json, urllib3
from urllib.parse import urlparse
from datetime import datetime
from requests.models import Response
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple
from requests.sessions import Session

from common.errors import *
import common.constants as constants


class RequestService(object):
    # protected members
    _session : Session = None
    _inside_cluster : bool = True
    _auth_url : str = None
    _base_url : str = None
    _user : str = None
    _password : str = None
    _verify : bool = False
    _cert_file_path : str = None
    _token : str = None
    _consul_token : str = None
    _session_details : dict = {}
    _client_details : str = None
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
                 client_details : Optional[str] = None, 
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
        self._user = user or ''
        self._password = password or ''
        self._verify = cert_verify # certificate verification flag
        self._cert_file_path = cert_file_path
        self._token = token
        self._consul_token = consul_token
        self._client_details = client_details
        self._tenant = tenant
        self._start_time = datetime.now()

        # Get token
        if self.__is_get_token(user, password, consul_token, token):
            self._token, self._session_details = self.__get_token(
                url = auth_url, 
                user = user,
                password = password,
                client_details = client_details)

        if self.__is_register_viya_client(consul_token, client_details, token):
            # self._token, self._session_details = self.__register_viya_client(
            #     consul_token = consul_token,
            #     url = url,
            #     client_details = client_details
            # )
            raise NotImplementedError(f"Client registration is not yet implemented")

        if self._token is None:
            raise AuthError("Token is not obtained")
        
        self.__set_auth_header(token = self._token)


    def __is_absolute(self, url : str): return bool(urlparse(url).netloc)
    

    def __set_auth_header(self, token : str):
        auth_header = { "Authorization": f"Bearer {token}" }
        self._session.headers.update(auth_header)


    def __get_token(self, 
                    url : str, 
                    user : str, 
                    password : str, 
                    client_details : Dict[str, str]
                   ) -> Tuple[str, dict]:
        """
        Obtains the access token by user and password.
        Returns access token and session details
        """
        headers = {'Accept': 'application/json',
                    'Content-Type': 'application/x-www-form-urlencoded'}
        payload = "grant_type=password&" \
                    f"username={user}" \
                    f"&password={password}"
        client_id = client_secret = ''
        if (client_details is not None 
            and len(str(client_details)) > 0):
            client_id, client_secret = client_details.split(":")
        authReturn : Response = self._session.post(
                url = url, 
                auth = (client_id, client_secret),
                data = payload, 
                headers = headers,
                verify = self._cert_file_path or self._verify)
        if (not authReturn.ok):
            raise AuthError("Failed to get access token", authReturn.text)

        session_details = authReturn.json()
        token = session_details['access_token']

        return token, session_details
    

    def __refresh_token(self) -> None:
        """
        The function refreshes token once we pass 80% of expires_in seconds.
        It then re-initialize bearer token and self.start_time
        :return:None
        """
        raise NotImplementedError
    

    def __register_viya_client():
        raise NotImplementedError

    
    def __is_get_token(self, user, password, consul_token, token) -> bool:
        return user and password and consul_token is None and token is None
    

    def __is_register_viya_client(self, consul_token, client_details, token) -> bool:
        return consul_token and client_details and token is None

    
    def __context_root_to_k8s_name(self, root = None):
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
            'templates': 'sas-templates'
        }
        return switcher.get(root, root)


    def __resolve_service_url(self, service = None) -> str:
        """
            returns the service url to use for the given context root.
            sys.exit if inputted context root is not found
        """
        k8s_service = self.__context_root_to_k8s_name(root = service)
        if service == k8s_service:
            RuntimeError(f"Unable to resolve service '{service}'.")

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
            if (isinstance(payload, dict) or isinstance(payload, list)):
                return json.dumps(payload)
        
        return payload
    
    
    def __handle_response(self, 
                          response : Response,
                          return_type = SimpleNamespace
                         ) -> Tuple[Any, Any]:
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise RequestError(http_status = e.response.status_code, server_response = str(e.response.text)) from None
        
        if (return_type and return_type in (dict, list)):
            return response.json(), response
        elif (return_type and return_type == SimpleNamespace):
            if response.text is not None and len(str(response.text)) > 0:
                return json.loads(response.text, object_hook = lambda d: SimpleNamespace(**d)), response
            return SimpleNamespace(), response
        elif (return_type):
            if response.text is not None and len(str(response.text)) > 0:
                return json.loads(response.text, object_hook = lambda d: return_type(**d)), response
            return return_type(), response
        else:
            return response.text, response


    # public members
    def get(self, 
            url,
            headers = {}, 
            params = {},
            return_type = SimpleNamespace
           ) -> Any:
        resp = self._session.get(
            url = self.__get_full_url(url),
            headers = headers,
            params = params,
            verify = self._cert_file_path or self._verify
            )

        return self.__handle_response(response = resp, return_type = return_type)


    def post(self,
             url,
             payload = {}, 
             headers = {}, 
             params = {},
             return_type = SimpleNamespace
            ):
        resp = self._session.post(
            url = self.__get_full_url(url),
            data = self.__convert_to_json_string(payload),
            headers = headers,
            params = params,
            verify = self._cert_file_path or self._verify
            )
        
        return self.__handle_response(response = resp, return_type = return_type)
    
    
    def put(self,
              url,
              payload = {}, 
              headers = {}, 
              params = {},
              return_type = SimpleNamespace
            ):
        resp = self._session.put(
            url = self.__get_full_url(url),
            data = self.__convert_to_json_string(payload),
            headers = headers,
            params = params,
            verify = self._cert_file_path or self._verify
            )
        return self.__handle_response(response = resp, return_type = return_type)


    def patch(self,
              url,
              payload = {}, 
              headers = {}, 
              params = {},
              return_type = SimpleNamespace
             ):
        resp = self._session.patch(
            url = self.__get_full_url(url),
            data = self.__convert_to_json_string(payload),
            headers = headers,
            params = params,
            verify = self._cert_file_path or self._verify
            )
        return self.__handle_response(response = resp, return_type = return_type)


