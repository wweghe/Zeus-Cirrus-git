/* ecl version */

%macro core_cas_initiate_session(cas_host =
                                 , cas_port =
                                 , cas_session_name = casauto
                                 , cas_session_id =
                                 , cas_session_options =
                                 , cas_assign_librefs = N );

   %local cas_session_id_option;

   /* Set default value for cas_session_name if it is missing */
   %if(%sysevalf(%superq(cas_session_name) =, boolean)) %then
      %let cas_session_name = casauto;

   /* Set options to connect to CAS Server */
   /* If cas_host and cas_port are provided, we connect to the that cas host/port.  Otherwise, the connection
   will automatically be to the default CAS server (generally cas-shared-default) */
   %if not(%sysevalf(%superq(cas_host) =, boolean) and %sysevalf(%superq(cas_port) =, boolean)) %then
      options cashost="&cas_host." casport=&cas_port. &cas_session_options. %str(;);
   %else %if not(%sysevalf(%superq(cas_session_options) =, boolean)) %then
      options &cas_session_options. %str(;);

   /* If cas_session_id is provided, add uuid= option so that we try to connect to that existing CAS session */
   %let cas_session_id_option=;
   %if not(%sysevalf(%superq(cas_session_id) =, boolean)) %then
      %let cas_session_id_option=uuid="&cas_session_id.";

   /* Start a CAS Session - if the CAS session (by name) already exists, don't try to re-assign (to avoid warnings) */
   %if not(%sysfunc(sessfound(&cas_session_name.))) %then %do;
      cas &cas_session_name. &cas_session_id_option.;

      %if not(%sysfunc(sessfound(&cas_session_name.))) %then %do;
         %if "&cas_session_id_option." ne "" %then
            %put ERROR: Failed to connect to CAS Session with ID "&cas_session_id.";
         %else %put ERROR: Failed to initiate the CAS Session "&cas_session_name.";
         %abort;
      %end;

   %end;
   %else %do;
      %put NOTE: A session with the name "&cas_session_name." already exists.;
   %end;

   /* Assign SAS librefs for all existing caslibs */
   %if "&cas_assign_librefs." = "Y" %then %do;
      caslib _all_ assign;
   %end;

%mend core_cas_initiate_session;