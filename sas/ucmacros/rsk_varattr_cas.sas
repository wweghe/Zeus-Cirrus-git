%macro rsk_varattr_cas(caslib=
                     , castable=
                     , cas_session_name=
                     , var=
                     , attr=
                     , out_var=attr_value       /* Name of a macrovariable to hold the requested variable attribute */
                     , out_table=   /* Name of a SAS dataset with all of the variable's attributes is created */
                     );
   %local keep_out_table;

   %if not %sysfunc(prxmatch(/^(length|formattedLength|type|label|format|nfl|nfd)$/i, %superq(attr))) %then %do;
      %put ERROR: input parameter ATTR = &attr. is invalid. Valid options are: length|formattedLength|type|label|format|nfl|nfd);
      %abort;
   %end;

   %let attr = %sysfunc(prxchange(s/formattedLength/FormattedLength/i, -1, &attr.));
   %let attr = %sysfunc(prxchange(s/length/RawLength/i, -1, &attr.));

   /* out_table cannot be missing. Set a default value */
   %let keep_out_table = Y;
   %if(%sysevalf(%superq(out_table) =, boolean)) %then %do;
      %let keep_out_table = N;
      %let out_table = var_attr_ds;
   %end;

   /* out_var cannot be missing. Set a default value */
   %if(%sysevalf(%superq(out_var) =, boolean)) %then
      %let out_var = attr_value;

   /* Declare the output variable as global if it does not exist */
   %if(not %symexist(&out_var.)) %then
      %global &out_var.;

   proc cas;
      %if(%sysevalf(%superq(cas_session_name) ne, boolean)) %then %do;
         session &cas_session_name.;
      %end;
      table.columninfo result=r /
      table={
         caslib="&caslib.",
         name="&castable.",
         %if(%sysevalf(%superq(var) ne, boolean)) %then %do;
            vars={{name="&var."}}
         %end;
      };
      /*symputx("&out_var.", r.columnInfo[1,"&attr."]);*/
      saveresult r dataout=&out_table.;
      ;
   quit;

   %if(%sysevalf(%superq(var) ne, boolean)) %then %do;
      data _null_;
         set &out_table.;
         call symputx("&out_var.", &attr.);
      run;
   %end;

   %if &keep_out_table.=N %then %do;
      proc datasets library = work nolist nodetails nowarn;
         delete &out_table.;
      quit;
   %end;

%mend;
