%macro core_get_authinfo_credentials(url =
                                    , outVarUser = username
                                    , outVarPwd = password
                                    , debug = false
                                    );

   %local
      authinfo_file
      regex_url
      protocol
      host
      port
      regex
      authinfo_port
      authinfo_user
      authinfo_pwd
      rc_fread
      fref
      fid
      str
      rc
   ;

   /* Make sure the parameter url is not missing */
   %if %sysevalf(%superq(url) =, boolean) %then %do;
      %put ERROR: input parameter url is missing;
      %abort;
   %end;

   /* Set a default value for the debug parameter (if missing) */
   %if %sysevalf(%superq(debug) =, boolean) %then
      %let debug = false;
   %else
      %let debug = %lowcase(&debug.);

   /* Set the regular expression to extract protocol, host and port from the url */
   %let regex_url = ((http[s]?):\/\/)?([^:\/]+)((:(\d+))|\/|$)(.*);
   /* Extract the protocol */
   %let protocol = %sysfunc(prxchange(s/&regex_url./\L$2/i, -1, %superq(url)));
   /* Extract the host */
   %let host = %sysfunc(prxchange(s/&regex_url./$3/i, -1, %superq(url)));
   /* Extract the port */
   %let port = %sysfunc(prxchange(s/&regex_url./$6/i, -1, %superq(url)));

   /* Set default protocol to http if it is missing */
   %if %sysevalf(%superq(protocol) =, boolean) %then
      %let protocol = http;

   /* Set default port if it is missing */
   %if %sysevalf(%superq(port) =, boolean) %then %do;
      %if(&protocol. = https) %then
         %let port = 443;
      %else %if(&protocol. = http) %then
         %let port = 80;
   %end;

   /* Set default value for outVarUser if it is missing */
   %if %sysevalf(%superq(outVarUser) =, boolean) %then
      %let outVarUser = username;

   /* Set default value for outVarPwd if it is missing */
   %if %sysevalf(%superq(outVarPwd) =, boolean) %then
      %let outVarPwd = password;

   /* Declare output variable outVarUser as global if it does not exist */
   %if not %symexist(&outVarUser.) %then
      %global &outVarUser.;

   /* Declare output variable outVarPwd as global if it does not exist */
   %if not %symexist(&outVarPwd.) %then
      %global &outVarPwd.;


   /* Determine the location of the AUTHINFO file. Precedence:
      1) AUTHINFO system option
      2) AUTHINFO environment variable
      3) NETRC environment variable
      4) Default AUTHINFO location
   */
   /* Check the AUTHINFO system option */
   %let authinfo_file = %sysfunc(getoption(authinfo));
   /* Check the AUTHINFO environment variable */
   %if %sysevalf(%superq(authinfo_file) =, boolean) %then %do;
      /* Check if the environment variable AUTHINFO exists */
      %if(%sysfunc(envlen(AUTHINFO)) > 0) %then
         %let authinfo_file = %sysget(AUTHINFO);
   %end;
   /* Check the NETRC environment variable */
   %if %sysevalf(%superq(authinfo_file) =, boolean) %then %do;
      /* Check if the environment variable NETRC exists */
      %if(%sysfunc(envlen(NETRC)) > 0) %then
         %let authinfo_file = %sysget(NETRC);
   %end;
   /* Use the default AUTHINFO location */
   %if %sysevalf(%superq(authinfo_file) =, boolean) %then %do;
      /* Determine the OS type */
      %if &SYSSCP. = WIN %then
         /* Windows OS -> use _authinfo */
         %let authinfo_file = %sysget(HOMEDRIVE)%sysget(HOMEPATH)/_authinfo;
      %else
         /* Linux OS -> use .authinfo */
         %let authinfo_file = ~/.authinfo;
   %end;

   %if %rsk_fileexist(&authinfo_file.) %then %do;
      /* Assign a filename to the authinfo file */
      %let fref = %rsk_get_unique_ref(path = &authinfo_file., debug = &debug.);
      /* Open the file */
      %let fid = %sysfunc(fopen(&fref.));
      %if(&fid. > 0) %then %do;

         /* Debug logging */
         %if(&debug. = true) %then %do;
            %put NOTE: Retrieving credentials for <host>:<port> --> &host.:&port.;
            %put NOTE: Processing AUTHINFO file: &authinfo_file.;
         %end;

         /* Set the file separator to be CR('0D'x) or LF('0A'x), forcing fread to read the entire line */
         %let rc = %sysfunc(fsep(&fid.,0D0A,x));
         %let rc_fread = 0;
         /* Loop through all records */
         %do %while(&rc_fread. = 0);
            /* Read a record to the file data buffer */
            %let rc_fread = %sysfunc(fread(&fid.));
            %if(&rc_fread. = 0) %then %do;
               %let str =;
               /* Copy the content of the file data buffer to the STR variable */
               %let rc = %sysfunc(fget(&fid., str));

               /* Debug logging */
               %if(&debug. = true) %then
                  %put NOTE: -> &str.;

               /* Regex to match the host in the authinfo entry */
               %let regex = ((^|.*\s)((default)|(host|machine)(\s+)%sysfunc(prxchange(s/\./\\./i, -1, %superq(host)))(\s|$))).*;
               /* Check if the current record matches the host */
               %if %sysfunc(prxmatch(/&regex./i, %superq(str))) %then %do;

                  /* We found a match on the host -> mark this so we can stop the looping */
                  %let rc_fread = 1;

                  /* Regex to match the port in the authinfo entry */
                  %let regex = ((^|.*\s)(port|protocol)\s+(\d+)).*;
                  /* Check if the current record matches the port */
                  %if %sysfunc(prxmatch(/&regex./i, %superq(str))) %then %do;
                     /* Extract the port */
                     %let authinfo_port = %sysfunc(prxchange(s/&regex./$4/i, -1, %superq(str)));
                     /* This authinfo entry specifies the port, we must match it against the url port */
                     %if %sysevalf(%superq(port) ne %superq(authinfo_port), boolean) %then
                        /* The url port does not match the authinfo port. Keep looping */
                        %let rc_fread = 0;
                  %end;

                  /* Regex to match the userid in the authinfo entry */
                  %let regex = ((^|.*\s)(user|login)\s+(([""]([^""]+)[""])|(['']([^'']+)[''])|([^\s]+))).*;
                  /* Check if the current record matches the userid */
                  %if %sysfunc(prxmatch(/&regex./i, %superq(str))) %then %do;
                     /* Extract the userid */
                     %let authinfo_user = %sysfunc(prxchange(s/&regex./$6$8$9/i, -1, %superq(str)));
                  %end;

                  /* Regex to match the password in the authinfo entry */
                  %let regex = ((^|.*\s)(password)\s+(([""]([^""]+)[""])|(['']([^'']+)[''])|([^\s]+))).*;
                  /* Check if the current record matches the password */
                  %if %sysfunc(prxmatch(/&regex./i, %superq(str))) %then %do;
                     /* Extract the password */
                     %let authinfo_pwd = %sysfunc(prxchange(s/&regex./$6$8$9/i, -1, %superq(str)));
                  %end;

                  /* If rc_fread = 1 then we have found valid match */
                  %if(&rc_fread.) %then %do;
                     %let &outVarUser. = &authinfo_user.;
                     %let &outVarPwd. = &authinfo_pwd.;

                     /* Debug logging */
                     %if(&debug. = true) %then
                        %put NOTE:   -> Match!;
                  %end;

               %end; /* If the current record matches the host */
            %end; /* %if(&rc_fread. = 0) */
         %end; /* Loop through all records */

         /* Close the file */
         %let rc = %sysfunc(fclose(&fid.));

      %end; /* %if(&fid. > 0) */
      %else %do;
         %put &ERROR.: Could not open file &authinfo_file.;
      %end;
      /* Deassign the filename */
      %let rc = %sysfunc(filename(fref));
   %end; /* %if %rsk_fileexist(&authinfo_file.) */

%mend;