%macro core_cas_terminate_session(cas_session_name = casauto);

   /* Set default value for cas_session_name if it is missing */
   %if(%sysevalf(%superq(cas_session_name) =, boolean)) %then
      %put ERROR: &cas_session_name. is required.;

   /* Terminate the CAS Session, if it exists */
   %if (%sysfunc(sessfound(&cas_session_name.))) %then %do;
      cas &cas_session_name. terminate;
   %end;

%mend core_cas_terminate_session;