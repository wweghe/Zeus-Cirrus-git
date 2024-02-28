/* Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA */

/*!
\file
\anchor core_set_base_url
\brief   The macro core_set_base_url.sas determines the base url for REST requests.

\param [in] host (optional) Host url.  Protocol is optional. (Ex: http://sas-risk-cirrus-objects)
\param [in] server Name that provides the REST service (Ex: riskCirrusObjects)
\param [in] port Server port (Ex: 80)
\param [out] outVarBaseUrl Name of the output macro variable that holds the determined base URL

\details
This macro determines the base URL to use for REST requests.  It uses this logic:

   If host is provided, we use the protocol in it (if it's included) and the service in it
   If host is not provided, we determine the protocol and service from server using these service naming patterns:

      Ex 1: server=SASRiskCirrus --> service=sas-risk-cirrus-app
      Ex 2: server=riskCirrusObjects --> service=sas-risk-cirrus-objects

   If protocol was not provided in the host, we get it from the SAS_URL_SERVICE_SCHEME environment variable

   If port is not provided, we get it from the port env var for the service or from the protocol if the port env var is not found:

      Ex 1: service=sas-risk-cirrus-object --> port=%sysget(SAS_RISK_CIRRUS_OBJECTS_SERVICE_PORT_HTTP)
      Ex 2: service=some-unknown-service -->
         protocol=https --> port=443
         protocol=http  --> port=80

<b>Example:</b>

\code
   %core_set_base_url(server=riskCirrusObjects);
   %put &=baseUrl; // for https environment, shows: https://sas-risk-cirrus-objects:443
\endcode

\ingroup macro utility
\author  SAS Institute Inc.
\date    2022
*/

%macro core_set_base_url(host=, server=, port=, outVarBaseUrl=baseUrl);

   /* Make sure output variable outVarBaseUrl is set */
   %if %sysevalf(%superq(outVarBaseUrl) =, boolean) %then
      %let outVarBaseUrl = baseUrl;

   /* Declare outVarBaseUrl as global if it does not exist */
   %if not %symexist(&outVarBaseUrl.) %then
      %global &outVarBaseUrl.;

   /* Initialize outVarBaseUrl */
   %let &outVarBaseUrl. =;

   /*************************************/
   /* 1. Determine service and protocol */
   /*************************************/

   /* if host not provided - determine service from server and protocol=$SAS_URL_SERVICE_SCHEME */
   /* if host provided - pull service from host.  if protocol in host, use it.  otherwise, protocol=$SAS_URL_SERVICE_SCHEME */
   %if %sysevalf(%superq(host) =, boolean) %then %do;

      %if %sysevalf(%superq(server) =, boolean) %then %do;
         %put ERROR: server is required if host is is not provided;
         %abort;
      %end;

      /* convert the service rest endpoint to the service name */
      /* ex: SASRiskCirrus --> sas-risk-cirrus-app */
      %if %sysfunc(prxmatch(/^SAS/, &server.))>0 %then %do;
         %let service=%lowcase(%sysfunc(prxchange(s/((^SAS)|([A-Z][a-z]+))/$1-/, -1, &server)))app;
      %end;
      /* ex: riskCirrusObjects --> sas-risk-cirrus-objects */
      %else %do;
         %let service=sas%lowcase(%sysfunc(prxchange(s/((^[a-z]+)|([A-Z][a-z]+))/-$1/, -1, &server)));
      %end;

      %if %sysfunc(sysexist(SAS_URL_SERVICE_SCHEME)) %then
         %let protocol=%sysget(SAS_URL_SERVICE_SCHEME);
   %end;
   %else %do;
      %let protocol=%sysfunc(prxchange(s/((https?):\/\/)?(.*)/$2/i, -1, %superq(host)));
      %let service=%sysfunc(prxchange(s/((https?):\/\/)?(.*)/$3/i, -1, %superq(host)));

      %if %sysevalf(%superq(protocol) =, boolean) %then %do;
         %if %sysfunc(sysexist(SAS_URL_SERVICE_SCHEME)) %then
            %let protocol=%sysget(SAS_URL_SERVICE_SCHEME);
      %end;
   %end;

   %if "&protocol."="" %then %do;
      %put ERROR: Protocol could not be determined.;
      %abort;
   %end;
   %if "&service."="" %then %do;
      %put ERROR: Service could not be determined.;
      %abort;
   %end;

   %put Determined Protocol: &protocol.;
   %put Determined Service: &service.;

   /*********************/
   /* 2. Determine port */
   /*********************/

   /* if port provided, use it.  otherwise:
      1. port = <service port http env var> - ex: for sas-risk-cirrus-object: port = $SAS_RISK_CIRRUS_OBJECTS_SERVICE_PORT_HTTP
      2. if env var not found, set to default values: port=80 for protocol=http or port=443 for protocol=https */
   %if %sysevalf(%superq(port) =, boolean) %then %do;
      %let service_port_var = %sysfunc(prxchange(s/-/_/, -1, %upcase(&service)))_SERVICE_PORT_HTTP;
      %if %sysfunc(sysexist(&service_port_var)) %then
         %let port=%sysget(&service_port_var);
      %if "&port."="" %then %do;
         %if &protocol.=http %then %let port=80;
         %else %let port=443;
      %end;
      %else %do;
         %if (&port=443 and &protocol. ne https) or (&port=80 and &protocol. ne http) %then
            %put WARNING: Port and protocol likely do not match: Port=&port., Protocol=&protocol.;
      %end;
   %end;
   %if "&port."="" %then %do;
      %put ERROR: Port could not be determined.;
      %abort;
   %end;

   %put Determined Port: &port.;

   %let &outVarBaseUrl.=&protocol://&service.:&port;
   %put Determined Base URL: &&&outVarBaseUrl..;

%mend;
