/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file
\anchor core_cas_upload_and_convert
\brief Create a new CAS table with all char variables converted to varchar variables from an input CAS table or input SAS datset.

\details

\param [in]  inLib : input SAS libref or CASlib
\param [in]  inTable : input SAS or CAS table name (1-level name)
\param [in]  inBasisCasLib : (optional) input CASlib for an existing CAS table to base column lenghts off of.
\param [in]  inBasisCasTable : (optional) input CAS table name of an existing CAS table to base column lenghts off of.
   If provided, all char columns in inTable being converted to varchar will have their length set to that same column's length
   in inBasisCasTable (if the column is a varchar in inBasisCasTable).
   This is useful if inTable will eventually be appended/joined to inBasisCasTable or a table like it later on.
\param [in]  encoding_adjustment : Whether or not to scale the length down based on the session encoding (Y/N) (Default: N)
   If Y, any char column lenghts not found in inBasisCasTable (if provided) will be divided by 4 (assuming the SAS session encoding is UTF8) to
   get the new varchar column length for that column.  Otherwise, the varchar column length stays the same.
   This is useful it an input SAS table was created from a CAS table, in which case varchar variables were converted to char variables and their
   lenghts are multiplied by 4 (assuming the SAS session encoding is UTF8)
\param [in] include_char_cols : (optional) space-separted list of character columns to be converted.
\param [in] excluded_char_cols : (optional) space-separted list of character columns to NOT be converted.
\param [in] casSessionName : name of an existing CAS session to use
\param [in] promoteResults : Whether or not outTable should be promoted to global scope (Y/N) (Default: N)
\param [in] custom_code : Any custom code to run in the CAS data step that creates the new CAS table.
\param [out]  outLib : output CASlib.
\param [out]  outTable : output CAS table name (1-level name)

WARNING: Converting character columns to varchar columns currently requires creating a new CAS table.  If working with large
tables, consider using this macro when creating the CAS table the first time, so that separate copies of the large CAS
table do not have to be created simply for converting character column types.  If any additional code needs run, it can
be specified in custom_code.

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2022
*/


%macro core_cas_upload_and_convert(inLib =
                                   , inTable =
                                   , inBasisCasLib =
                                   , inBasisCasTable =
                                   , outLib =
                                   , outTable =
                                   , encoding_adjustment = N
                                   , include_char_cols =
                                   , exclude_char_cols =
                                   , casSessionName =
                                   , promoteResults = N
                                   , custom_code =
                                   );

   %local   inLibref outCasLibref inLibrefEngine
            divisor
            varchar_cols_basis varchar_lens_basis
            varchar_cols varchar_lens varchar_cols_rename_stmt varchar_cols_len_stmt varchar_cols_assign_stmt
            quoted_include_char_cols quoted_exclude_char_cols
            col i j
            ;

   %let inLibrefEngine = %rsk_get_lib_engine(&inLib.);

   %let inLibref=&inLib.;
   %if "&inLibrefEngine." = "CAS" %then
      %let inLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&inLib." sessref=&casSessionName.);
   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outLib." sessref=&casSessionName.);

   %if "&include_char_cols." ne "" %then
      %let quoted_include_char_cols=%sysfunc(prxchange(s/(\w+)/"$1"/, -1, %upcase(&include_char_cols.)));

   %if "&exclude_char_cols." ne "" %then
      %let quoted_exclude_char_cols=%sysfunc(prxchange(s/(\w+)/"$1"/, -1, %upcase(&exclude_char_cols.)));

   %let divisor=1;
   %if %upcase("&encoding_adjustment") = "Y" %then %do;
      %if "%sysfunc(getoption(encoding))" = "UTF8" %then
         %let divisor=4;
   %end;

   %if "&inBasisCasTable." ne "" %then %do;

      proc cas;
         session &casSessionName.;
         table.columnInfo result = r / table ={
            caslib="&inBasisCasLib." name="&inBasisCasTable."
         };
         saveresult r dataout=work.basis_table_info;
      quit;

      proc sql noprint;
         select distinct upcase(column), round(rawlength) into
            :varchar_cols_basis separated by ' ',
            :varchar_lens_basis separated by ' '
         from work.basis_table_info
         where type="varchar"
         %if "&include_char_cols." ne "" %then %do;
            and upcase(column) in (&quoted_include_char_cols.)
         %end;
         %if "&exclude_char_cols." ne "" %then %do;
            and upcase(column) not in (&quoted_exclude_char_cols.)
         %end;
         ;
      quit;

   %end;

   %if "&inLibrefEngine." = "CAS" %then %do;

      proc cas;
         session &casSessionName.;
         table.columnInfo result = r / table ={
            caslib="&inLib." name="&inTable."
         };
         saveresult r dataout=work.table_info;
      quit;

      proc sql noprint;
         select distinct upcase(column), round(rawlength/&divisor.) into
            :varchar_cols separated by ' ',
            :varchar_lens separated by ' '
         from work.table_info
         where type="char"
         %if "&include_char_cols." ne "" %then %do;
            and upcase(column) in (&quoted_include_char_cols.)
         %end;
         %if "&exclude_char_cols." ne "" %then %do;
            and upcase(column) not in (&quoted_exclude_char_cols.)
         %end;
         ;
      quit;

   %end;
   %else %do;

      proc sql noprint;
         select distinct upcase(name), round(length/&divisor.) into
            :varchar_cols separated by ' ',
            :varchar_lens separated by ' '
         from sashelp.vcolumn
         where libname=upcase("&inLib.") and memname=upcase("&inTable.") and type="char"
         %if "&include_char_cols." ne "" %then %do;
            and upcase(column) in (&quoted_include_char_cols.)
         %end;
         %if "&exclude_char_cols." ne "" %then %do;
            and upcase(column) not in (&quoted_exclude_char_cols.)
         %end;
         ;
      quit;

   %end;

   %let varchar_cols_rename_stmt=;
   %let varchar_cols_len_stmt=;
   %let varchar_cols_assign_stmt=;
   %do i = 1 %to %sysfunc(countw(&varchar_cols., %str( )));
      %let col = %scan(&varchar_cols., &i., %str( ));
      %let varchar_cols_rename_stmt = &varchar_cols_rename_stmt. _varchar_col_&i. = &col.;

      %if %sysfunc(findw(&varchar_cols_basis., &col.)) %then %do;
         %do j = 1 %to %sysfunc(countw(&varchar_cols_basis., %str( )));
            %if %scan(&varchar_cols_basis., &j., %str( ))=&col. %then
               %let varchar_cols_len_stmt = &varchar_cols_len_stmt. _varchar_col_&i. varchar(%scan(&varchar_lens_basis., &j., %str( )));
         %end;
      %end;
      %else
         %let varchar_cols_len_stmt = &varchar_cols_len_stmt. _varchar_col_&i. varchar(%scan(&varchar_lens., &i., %str( )));

      %let varchar_cols_assign_stmt = &varchar_cols_assign_stmt. _varchar_col_&i. = strip(%scan(&varchar_cols., &i., %str( )))%str(;);
   %end;

   data &outCasLibref..&outTable. (rename=(&varchar_cols_rename_stmt.)
      %if "&promoteResults." = "Y" %then %do;
         promote=yes
      %end;
      );

      length &varchar_cols_len_stmt.;
      set &inLibref..&inTable.;

      %if(%sysevalf(%superq(custom_code) ne, boolean)) %then %do;
         %unquote(&custom_code.);
      %end;

      &varchar_cols_assign_stmt.
      drop &varchar_cols.;
   run;

   %if "&inLibrefEngine." = "CAS" %then %do;
      libname &inLibref. clear;
   %end;
   libname &outCasLibref. clear;

%mend;