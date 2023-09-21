%macro core_cas_drop_table(cas_session_name =
                           , cas_session_id =
                           , cas_session_options =
                           , cas_libref =
                           , cas_table =
                           , delete_table = Y
                           , delete_table_options = quiet=TRUE
                           , verify_table_deleted = Y
                           , delete_source = Y
                           , delete_source_options = quiet=TRUE);

   /* Delete the CAS table in-memory, if requested */
   %if "&delete_table." = "Y" %then %do;

      proc cas;
         session &cas_session_name.;
         table.droptable / caslib="%superq(cas_libref)" name="%superq(cas_table)" &delete_table_options.;
      quit;

      /* for some reason, this is needed to ensure CAS tables created by RSM API export are actually deleted */
      %rsk_dsexist_cas(cas_lib=%superq(cas_libref),cas_table=%superq(cas_table),cas_session_name=&cas_session_name.);
      %if &cas_table_exists. %then %do;
         proc cas;
            session &cas_session_name.;
            table.droptable / caslib="%superq(cas_libref)" name="%superq(cas_table)" &delete_table_options.;
         quit;
      %end;

      %if "&verify_table_deleted." = "Y" %then %do;
         %rsk_dsexist_cas(cas_lib=%superq(cas_libref),cas_table=%superq(cas_table),cas_session_name=&cas_session_name.);
         %if &cas_table_exists. %then %do;
            %put ERROR: Failed to delete table &cas_libref..&cas_table.;
            %abort;
         %end;
      %end;

   %end;

   /* Delete the CAS table on-disk, if requested */
   %if "&delete_source." = "Y" %then %do;

      proc cas;
         session &cas_session_name.;
         table.deleteSource / caslib="%superq(cas_libref)" source="%superq(cas_table).sashdat" &delete_source_options.;
      quit;

   %end;

%mend core_cas_drop_table;