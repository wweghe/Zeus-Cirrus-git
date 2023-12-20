/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_append
\brief The macro rsk_append.sas stacks the set of partitioned tables into one non-partitioned table.

\details

<b> Identified Inputs </b>

The following inputs are required by this macro:
  \param[in] base              The base dataset to which records are appended
  \param[in] data              The dataset to append to the base dataset
  \param[in] length_selection  Choices are 'base', 'data', or 'longest'. Default is 'base'. When a column has 2 different lengths in the
                               two datasets, this determines which length to use.
  \param[in] options           Proc append options to use. Defaults to force nowarn
  \param[in] where_clause      Where clause to associate with the data being appended to the base dataset (see example)


\author SAS Institute INC.
\date 2020

 */
%macro rsk_append( base = 
                 , data =
                 , length_selection = base
                 , options = force nowarn
                 , where_clause =
                 );
                   
   %local 
      where_statement
      TotModStatements
      TotAddStatements
      i
      ;

   %if (%rsk_dsexist(&data.)) %then %do;
      %if (%rsk_dsexist(&base.)) %then %do;
         /* Compare columns from base and data. If they share a column but that column has different lengths, 
            use the length according to the length_selection parameter */
         proc contents noprint data= &base. out=_attrs_base(KEEP=name length type memname);
         run;

         proc contents noprint data= &data. out=_attrs_data(KEEP=name length type memname);
         run;

         data _mods_base _mods_data;
            merge _attrs_base(rename=(length=length_base)) _attrs_data(rename=(length=length_data));
            length length_final 8 mod_statement $512 add_statement $512;
            by name;
            if not missing(length_base) and not missing(length_data) and length_data ne length_base then do;
               if lowcase("&length_selection.") eq "longest" then
                  length_final = max(length_base,length_data);
               else if lowcase("&length_selection.") eq "data" then
                  length_final = length_data;
               else
                  length_final = length_base;
               
               if type eq 2 then 
                  mod_statement = strip(Name) || " char(" || strip(length_final) || ")";
               else
                  mod_statement = strip(Name) || " length = " || strip(length_final);
                  

               if lowcase("&length_selection.") eq "longest" then
                  if length_final gt length_base then
                     output _mods_base;
                  else
                     output _mods_data;
               else if lowcase("&length_selection.") eq "data" then
                  output _mods_base;
               else
                  output _mods_data;
            end;
            /* Create add statement for when base is missing a column in the data */
            else if missing(length_base) and not missing(length_data) then do;
               length_final = length_data;
               if type eq 2 then
                  add_statement = strip(Name) || " char(" || strip(length_final) || ")";
               else
                  add_statement = strip(Name) || " length = " || strip(length_final);
               output _mods_base;
            end;            
         run;

         %if(%rsk_attrn(_mods_data, nobs) ne 0) %then %do;
            data _data_mod;
               set &data.;
            run;
            %let data = _data_mod;

            %let TotModStatements = 0;
            data _null_;
               set _mods_data(where = (mod_statement is not missing)) end = last;
               call symputx(cats("mod_statement_", put(_N_, 8.)), mod_statement, "L");
               if last then
                  call symputx("TotModStatements", _N_, "L");
            run;
            
            %if(&TotModStatements. > 0) %then %do;
               proc sql noprint;
                  alter table &data.
                  modify
                     %do i = 1 %to &TotModStatements.;
                        %if(&i. > 1) %then 
                           ,
                        ;
                        &&mod_statement_&i..
                     %end;
                  ;
               quit;
            %end;
         %end;

         %if(%rsk_attrn(_mods_base, nobs) ne 0) %then %do;
         
            %let TotModStatements = 0;
            %let TotAddStatements = 0;
            data _null_;
               set _mods_base end = last;
               retain mod_cnt add_cnt 0;
               if(not missing(mod_statement)) then do;
                  mod_cnt + 1;
                  call symputx(cats("mod_statement_", put(mod_cnt, 8.)), mod_statement, "L");
               end;
               if(not missing(add_statement)) then do;
                  add_cnt + 1;
                  call symputx(cats("add_statement_", put(add_cnt, 8.)), add_statement, "L");
               end;
               if last then do;
                  call symputx("TotModStatements", mod_cnt, "L");
                  call symputx("TotAddStatements", add_cnt, "L");
               end;
            run;

            %if(&TotModStatements. > 0) %then %do;
               proc sql noprint;
                  alter table &base.
                  modify
                     %do i = 1 %to &TotModStatements.;
                        %if(&i. > 1) %then 
                           ,
                        ;
                        &&mod_statement_&i..
                     %end;
                  ;
               quit;
            %end;

            /* Add any columns that are in data but not base to the base table to avoid warnings when appending */
            %if(&TotAddStatements. > 0) %then %do;
               proc sql noprint;
                  alter table &base.
                  add
                     %do i = 1 %to &TotAddStatements.;
                        %if(&i. > 1) %then 
                           ,
                        ;
                        &&add_statement_&i..
                     %end;
                  ;
               quit;
            %end;

         %end; /* %if(%rsk_attrn(_mods_base, nobs) ne 0) */
      %end; /* %if (%rsk_dsexist(&base.)) */

      %let where_statement =;
      %if(%sysevalf(%superq(where_clause) ne, boolean)) %then %do;
         %let where_statement = (where = (%superq(where_clause)));
      %end;

      proc append
         base = &base.
         data = &data. &where_statement.
         &options.;
      run;
      
      /* Remove temporary data artefacts from the WORK */
      proc datasets library = work
                    memtype = (data)
                    nolist nowarn;
         delete _attrs_base _attrs_data _mods_base _mods_data _data_mod;
      quit;
   %end; /* %if (%rsk_dsexist(&data.)) */
%mend;
