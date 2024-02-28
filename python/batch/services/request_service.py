import requests, os, json, urllib3, base64, http.client
import jwt, ssl
from urllib.parse import urlparse
from datetime import datetime, timedelta
from requests.models import Response
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple, Callable
from requests.sessions import Session

from common.errors import *
import common.constants as constants

from domain.access_session import AccessSession
from domain.state import SharedStateProxy, SharedStateKeyEnum


class RequestService(object):
    
    __AUTH_HEADERS = {'Accept': 'application/json',
                      'Content-Type': 'application/x-www-form-urlencoded'}
    __SAS_LOGON_URL = "/SASLogon"
    __OAUTH_TOKEN_URL = "/oauth/token"
    __OPENID_CONFIGURATION_URL = "/.well-known/openid-configuration"
    __GRANT_TYPE_TO_REFRESH_TOKEN = "urn:ietf:params:oauth:grant-type:jwt-bearer" # NGMTS-34176: avoid "user_token" 
    __REFRESH_TOKEN_SKEW_MIN_SEC = 15
    # __AUTH_TOKEN_URI = "/oauth2/default/v1/token"

    _session : Session = None
    _inside_cluster : bool = True
    _auth_url : str = None
    _oidc_url : str = None
    _refresh_token_url : str = None
    _base_url : str = None
    _user : str = None
    _password : str = None
    _verify : bool = False
    _cert_file_path : str = None
    _token : str = None
    _token_decoded : Dict[str, Any] = None
    _refresh_token : str = None
    _session_details : dict = {}
    _client_auth : Tuple[str, str] = None
    _tenant : str = None

    _state : SharedStateProxy = None
    _access_session : AccessSession = None

    # private members
    def __init__(self, 
                 url : str, 
                 shared_state :  SharedStateProxy,
                 auth_url : Optional[str] = None,
                 oidc_url : Optional[str] = None,
                 user : Optional[str] = None, 
                 password : Optional[str] = None, 
                 token : Optional[str] = None,
                 client_auth : Optional[str] = None, 
                 cert_verify : bool = True,
                 cert_file_path : str = None,
                 tenant : Optional[str] = None,
                 disable_insecure_warning : Optional[bool] = False,
                 logger_debug_func : Any = None
                ) -> None:
        
        if shared_state is None: raise ValueError(f"shared_state cannot be empty")

        if logger_debug_func:
            http.client.HTTPConnection.debuglevel = 1
            http.client.print = logger_debug_func

        self._inside_cluster = url is None or len(str(url)) == 0
        app_url = self.__resolve_service_url(service = "sas-risk-cirrus-core").rstrip("/") if self._inside_cluster else url.rstrip("/")
        logon_url = self.__resolve_service_url(service = 'SASLogon').rstrip('/') if self._inside_cluster else url.rstrip('/')

        self._base_url = app_url
        self._auth_url = f"{logon_url}{self.__SAS_LOGON_URL}{self.__OAUTH_TOKEN_URL}" if auth_url is None else auth_url
        self._oidc_url = f"{logon_url}{self.__SAS_LOGON_URL}{self.__OPENID_CONFIGURATION_URL}" if oidc_url is None else oidc_url
        self._refresh_token_url = self._auth_url
        self._user = user or ''
        self._password = password or ''
        self._verify = cert_verify # certificate verification flag
        self._cert_file_path = cert_file_path
        self._token = token
        self._tenant = tenant
        self._state = shared_state
        self._disable_insecure_warning = disable_insecure_warning
        
        if (client_auth is not None and len(str(client_auth)) > 0):
            client_id, client_secret = client_auth.split(":")
            self._client_auth = (client_id, client_secret)

        if self._client_auth is None and token is None:
            raise ValueError(f"client_auth cannot be empty with password authentiation")
        if self._client_auth is None and not self._inside_cluster:
            raise ValueError(f"client_auth cannot be empty with token authentiation outside of the cluster")
        
        if disable_insecure_warning:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self._session = requests.session()

        # Get token
        if token is None:
            self._token, self._refresh_token, self._session_details = self.__get_token_using_password( 
                user = user,
                password = password,
                client_auth = self._client_auth)
            self._token_decoded = self.__decode_token(self._token, verify_signature = False)
            self._access_session = AccessSession(
                access_token = self._token,
                token_decoded = self._token_decoded,
                refresh_token = self._refresh_token,
                session_details = self._session_details,
                client_auth = self._client_auth
            )

        else: # auth token was passed instead of user and password
            # IMPORTANT: extract client auth from token, only works inside cluster due to consul
            if self._inside_cluster:
                self._token_decoded = self.__decode_token(token)
                self._client_auth = self.__get_client_auth_from_token(self._token_decoded)
            # force refresh token to make sure we are not expiring and we have access_session
            self._token, self._refresh_token, self._session_details = self.__refresh_token(
                token = token, 
                client_auth = self._client_auth
                )
            self._token_decoded = self.__decode_token(token)

            self._access_session = AccessSession(
                access_token = self._token,
                token_decoded = self._token_decoded,
                refresh_token = self._refresh_token,
                session_details = self._session_details,
                client_auth = self._client_auth
            )

        if self._token is None:
            raise AuthError("Token is not obtained")
        
        self.__set_auth_header(token = self._token)
        self.__update_access_session(self._access_session)


    def __get_access_session(self) -> AccessSession:
        
        self._state.lock()

        try:
            return self._state.get(SharedStateKeyEnum.ACCESS_SESSION, self._access_session)
        finally:
            self._state.unlock()
    

    def __update_access_session(self, access_session : AccessSession) -> None:
        
        self._state.lock()

        try:
            self._state.update(SharedStateKeyEnum.ACCESS_SESSION, access_session)
        finally:
            self._state.unlock()
    
    
    def __is_absolute(self, url : str): return bool(urlparse(url).netloc)
    

    def __set_auth_header(self, token : str):
        auth_header = { "Authorization": f"Bearer {token}" }
        self._session.headers.update(auth_header)


    def __decode_token(self, 
                         token : str, 
                         verify_signature : bool = True, 
                         verify_exp : bool = True,
                         verify_iat : bool = True,
                         audience : str = "openid" # https://www.rfc-editor.org/rfc/rfc7519#section-4.1.3 ; audience values is generally application specific ; ideally it should be client_id
                        ) -> Dict[str, Any]:

        if not verify_signature:
            token_header = jwt.get_unverified_header(token)
            return jwt.decode(
                jwt = token, 
                algorithms = token_header.get("alg", None), 
                verify = True, 
                options = { "verify_signature": verify_signature })

        headers = { "Authorization": f"Bearer {token}" }
        oidc_config_resp : Response = self._session.get(
                url = self._oidc_url, 
                headers = headers,
                verify = self._cert_file_path or self._verify)
        if (not oidc_config_resp.ok):
            raise AuthError("Failed to get openid configuration.", oidc_config_resp.text)

        oidc_config = oidc_config_resp.json()
        jwks_uri = oidc_config.get("jwks_uri", None)
        signing_algos = oidc_config.get("id_token_signing_alg_values_supported", None)
        ssl_ctx = ssl.create_default_context()
        
        if self._cert_file_path is not None:
            ssl_ctx.load_verify_locations(self._cert_file_path)
        elif self._cert_file_path is None and not self._verify:
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE

        jwks_client = jwt.PyJWKClient(jwks_uri, ssl_context = ssl_ctx)
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        
        return jwt.decode(
            jwt = token,
            key = signing_key.key,
            algorithms = signing_algos,
            # audience = audience,
            options = { 
                "verify_exp": verify_exp, 
                "verify_iat": verify_iat,
                "verify_aud": False,
                "verify_iss": False,
                "verify_nbf": False
                }
            )

    def __get_token_using_jwt(self, 
                              token : str, 
                              client_auth : Tuple[str, str]
                             ) -> Tuple[str, str, dict]:
        
        payload = f"grant_type={self.__GRANT_TYPE_TO_REFRESH_TOKEN}" \
            f"&assertion={token}"
        authReturn : Response = self._session.post(
            url = self._auth_url, 
            auth = client_auth,
            data = payload, 
            headers = self.__AUTH_HEADERS,
            verify = self._cert_file_path or self._verify)
        if (not authReturn.ok):
            raise AuthError("Failed to get token using jwt.", authReturn.text)

        session_details = authReturn.json()
        access_token = session_details.get('access_token', None)
        refresh_token = session_details.get('refresh_token', None)

        return access_token, refresh_token, session_details
    

    def __get_token_using_refresh_token(self,
                                        token : str,
                                        client_auth : Tuple[str, str],
                                        refresh_token : str = None
                                       ) -> Tuple[str, str, dict]:
        if refresh_token is None or len(str(refresh_token)) == 0:
            refresh_token, _ = self.__get_refresh_token(token, client_auth)
        payload = f"grant_type=refresh_token" \
                f"&refresh_token={refresh_token}"
            
        authReturn = self._session.post(
            url = self._refresh_token_url, 
            auth = client_auth,
            data = payload, 
            headers = self.__AUTH_HEADERS,
            verify = self._cert_file_path or self._verify)

        if not authReturn.ok:
            raise AuthError("Failed to get access token using refresh token", authReturn.text)

        session_details = authReturn.json()
        access_token = session_details.get('access_token', None)
        refresh_token = session_details.get('refresh_token', None)

        return access_token, refresh_token, session_details
    

    def __get_refresh_token(self, 
                            token : str, 
                            client_auth : Tuple[str, str]
                           ) -> Tuple[str, dict]:

        headers = { "Authorization": f"Bearer {token}" }
        headers.update(self.__AUTH_HEADERS)
        client_id = client_auth[0]
        payload = f"grant_type={self.__GRANT_TYPE_TO_REFRESH_TOKEN}" \
            f"&client_id={client_id}"
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
    

    def __refresh_token(self, 
                        token : str,
                        client_auth : Tuple[str, str],
                        refresh_token : str = None
                       ) -> Tuple[str, str, dict]:
        
        access_token : str = None
        refresh_token : str = None
        session_details : Dict[str, Any] = {}

        try:
            access_token, refresh_token, session_details = self.__get_token_using_jwt(token, client_auth)
        except AuthError as e:
            pass
        
        if access_token is None:
            return self.__get_token_using_refresh_token(token, client_auth, refresh_token)

        return access_token, refresh_token, session_details


    def __get_token_using_password(self, 
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


    def __remove_client_id_prefix(self, client_id : str, prefix : str = "sas.") -> str:
        
        if client_id is not None and client_id.startswith(prefix):
            return client_id[len(prefix):]
        return client_id


    def __get_client_auth_from_token(self, token_decoded : Dict[str, Any]) -> Tuple[str, str]:
        client_id = token_decoded.get("client_id", None)
        if client_id is None:
            raise AuthError("Token does not contain client id.")
        client_secret = self.__get_client_id_secret_from_consul(client_id)
        return (client_id, client_secret)
    
    
    def __get_client_id_secret_from_consul(self, client_id = constants.CLIENT_ID_RISK_CIRRUS_CORE) -> str:
        """
        Function retrieves client_id's secret as string from consul.

        Args:
            client_id: id of the client. 
        Returns:
            client secret
        """
        
        consul_http_token = os.environ.get('CONSUL_HTTP_TOKEN')
        base_url = self.__resolve_service_url(service = "consul")
        client_id_no_prefix = self.__remove_client_id_prefix(client_id)

        if consul_http_token is None or len(str(consul_http_token)) == 0:
            raise AuthError(f"Failed to get client id '{client_id_no_prefix}' secret from consul. Reason: environmental variable 'CONSUL_HTTP_TOKEN' is empty.")

        url = f"{base_url}/v1/kv/config/{client_id_no_prefix}/oauth2.client.clientSecret"
        header = {"X-Consul-Token": consul_http_token}
        resp = self._session.get(
            url, 
            headers = header, 
            verify = self._cert_file_path or self._verify
            )
        if not resp.ok:
            if resp.status_code == 404:
                return ""
            raise AuthError(f"Failed to get client id '{client_id_no_prefix}' secret from consul.", resp.text)
        
        secrets = resp.json()
        if len(secrets) == 0:
            return ""
        else:
            return base64.b64decode(secrets[0].get('Value')).decode("utf-8")


    def __get_token_expiration_from_now(self, token_decoded : Dict[str, Any] = None) -> int:

        if token_decoded is None:
            token_decoded = self.__decode_token(self._token)

        exp = token_decoded.get("exp")
        exp_dttm = datetime.utcfromtimestamp(exp)
    
        return (exp_dttm - datetime.utcnow()).total_seconds()
    
    
    def __is_token_expiring(self, refresh_skew_sec : int = __REFRESH_TOKEN_SKEW_MIN_SEC) -> bool:
        # if refresh_skew_sec is None or refresh_skew_sec < self.__REFRESH_TOKEN_SKEW_MIN_SEC:
        #     refresh_skew_sec = self.__REFRESH_TOKEN_SKEW_MIN_SEC
        return (self.__get_token_expiration_from_now(self._token_decoded) - refresh_skew_sec) <= 0


    def get_new_access_session(self) -> AccessSession:
        
        access_token : str = None
        refresh_token : str = None
        session_details : Dict[str, Any] = {}

        try:
            access_token, refresh_token, session_details = self.__get_token_using_jwt(
                self._access_session.access_token, 
                self._client_auth)
        except AuthError as e:
            pass
        
        if access_token is None:
            access_token, refresh_token, session_details = self.__get_token_using_refresh_token(
                self._access_session.access_token, 
                self._client_auth, 
                self._refresh_token)
        
        token_decoded = self.__decode_token(access_token, verify_signature = False)

        return AccessSession(
            access_token = access_token,
            refresh_token = refresh_token,
            session_details = session_details,
            client_auth = self._client_auth,
            token_decoded = token_decoded
        )
    

    def __refresh_access_session_if_expiring(self) -> None:

        if self.__is_token_expiring():
            self._access_session = self.__get_access_session()

            self._token = self._access_session.access_token
            self._refresh_token = self._access_session.refresh_token
            self._token_decoded = self._access_session.token_decoded
            self._session_details = self._access_session.session_details
            
            if self._token is None:
                raise AuthError("Failed to refresh token.")
            
            self._token_decoded = self.__decode_token(self._token)
            
            self.__set_auth_header(token = self._token)


    # def get_access_expiration_from_issuedAt(self) -> int:

    #     token_decoded = self._token_decoded
    #     if self._token_decoded is None:
    #         token_decoded = self.__decode_token(self._token)
    #     exp = iat = token_decoded.get("exp")
    #     exp_dttm = datetime.utcfromtimestamp(exp)
    #     iat = token_decoded.get("iat")
    #     iat_dttm = datetime.utcfromtimestamp(iat)

    #     return (exp_dttm - iat_dttm).total_seconds()


    # def get_access_expiration_from_now(self) -> int:

    #     return self.__get_token_expiration_from_now()
    

    # def is_access_expiring(self) -> bool:
    #     return self.__is_token_expiring()


    def get(self, 
            url,
            headers = {}, 
            params = {},
            return_type = SimpleNamespace,
            return_conversion_func : Callable[[str], Any] = None
           ) -> Any:
        self.__refresh_access_session_if_expiring()
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
        self.__refresh_access_session_if_expiring()
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
        self.__refresh_access_session_if_expiring()
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
        self.__refresh_access_session_if_expiring()
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
    

    def reinit(self) -> None:
        if self._disable_insecure_warning:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
