/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/*!
\file
\anchor core_upload_to_cas
\brief   Load a SAS table to the CAS Server.

\param [in] cas_session_options name of a cas session to connect
\param [in] list of cas session options for the session name
\param [in] DS_IN Input data set to be loaded
\param [in] CAS_LIBRARY_NAME Metadata Name of the CAS library
\param [in] TARGET_TABLE_NM Name of the target table
\param [in] MODE Determines how the table is loaded into VA. Values: APPEND/REPLACE (Default: REPLACE)
\param [in] ERROR_IF_DS_IN_NOT_EXIST (Optional) Flag (YES/NO) to specify whether the macro should error and return if &ds_in. does not exist.  If this flag is YES, then an error will be thrown if &ds_in. does not exist.  If this flag is NO, then the macro will continue and will delete all records from the CAS table if &ds_in. does not exist and mode is REPLACE.

\details This macro will load the input table to the specified CAS libarary.

\ingroup utilities
\author  SAS Institute Inc.
\date    2021
*/

%macro core_cas_upload_table(cas_session_name =
                           , ds_in =
                           , cas_library_name =
                           , target_table_nm =
                           , mode = replace
                           , error_if_ds_in_not_exist = YES
                           );

   %local
      append_opt
   ;

   /* Make sure the mode parameter is set */
   %if %sysevalf(%superq(mode) =, boolean) %then
      %let mode = REPLACE;

   %else
      %let mode = %upcase(&mode.);

   /* Validate the MODE parameter */
   %if not %sysfunc(prxmatch(/^(APPEND|REPLACE)$/i, %superq(mode))) %then %do;
      %put ERROR: input parameter mode = &mode. is invalid. Valid values are APPEND|REPLACE;
      %abort;
   %end;

   /* Assign libref tmp_cas to casLib */
   libname tmp_cas cas caslib="&cas_library_name.";

   /* Check if there is anything to load */
   %if not %rsk_dsexist(&ds_in.) %then %do;

      /* Check if we need to throw an error or should continue */
      %if &error_if_ds_in_not_exist. = YES %then %do;

         /* Throw an error and exit (avoid attempting to load LASR and update the metadata) */
         %put ERROR: Input table &ds_in. does not exist. Skipping data load to CAS.;
         %abort;

      %end;
      %else %do;

         /* If we are in REPLACE mode and the CAS table exists then we should just delete all records from CAS */
         %rsk_dsexist_cas(cas_lib=%superq(cas_library_name),cas_table=%superq(target_table_nm),cas_session_name=&cas_session_name.);
         %if(&cas_table_exists. and &mode. = REPLACE) %then %do;

            data tmp_cas.&target_table_nm. (promote=yes);
               set tmp_cas.&target_table_nm. (obs=0);
            run;

         %end;

         %else %do;

            /* There is nothing to do. Just print a log message and exit */
            %put NOTE: Input table &ds_in. does not exist. Skipping data load to CAS.;
            %return;

         %end;

      %end;

   %end;
   %else %do;

      /* Either the target table does not already exist or it is to be replaced */
      %if &mode. = REPLACE %then %do;

         /* Drop the existing table so that the new one can be promoted */
         proc cas;
            session &cas_session_name.;
            table.droptable / caslib="%superq(cas_library_name)" name="%superq(target_table_nm)" quiet=TRUE;
         quit;

         /* Re-create and promote the new table */
         data tmp_cas.&target_table_nm. (promote=yes);
            set &ds_in.;
         run;

      %end;

      /* Append input dataset to existing target table */
      %else %if &mode. = APPEND %then %do;

         /* Load input dataset as a temporary CAS table */
         data casuser.cas_table_to_append;
            set &ds_in.;
         run;

         /* Appened temporary CAS table to promoted CAS table */
         %if %rsk_dsexist(tmp_cas.&target_table_nm.) %then
            /* Promoted table already exists */
            /* Append the temporary CAS table to the existing promoted CAS table */
            %let append_opt = append;

         %else
            /* Promoted table does not exist */
            /* Load the temporary CAS table as a promoted CAS table */
            %let append_opt = promote;

         data tmp_cas.&target_table_nm. (&append_opt.=yes);
            set casuser.cas_table_to_append;
         run;

      %end;

   %end;

   libname tmp_cas clear;

%mend;