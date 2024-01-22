/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file 
\anchor rsk_dir_list
   \brief   Directory listing

   \param [in] directory Directory or list of comma separated directories to look for
   \param [in] ds_out Output table name
   \param [in] recursive Should the search be performed recursively on sub directories? Y/N
   \param [in] filemask Defines which files should be listed. Looking for sas files: *.sas -> In general: *word1*word2*.txt
   \param [in] regexp Should a custom regular expression used instead of a simple wildcard matching? Y/N. If Y, filemask must be a valid regular expression (WITHOUT the forward slashes -> /regexp/)
   \param [in] case_sensitive Should the match be case sensitive? Y/N

   \details This macro performs a directory listing of all files matching the filemask or a given regular expression. Directory recursion is also possible

   The structure of the output table is the following:

   | Variable           | Type            | Description           |
   |--------------------|-----------------|-----------------------|
   | PARENT_DIR         | CHARACTER(2000) | Parent directory      |
   | ITEM_TYPE          | CHARACTER(10)   | Item type: File       |
   | FILE_PATH          | CHARACTER(2000) | Full path to the file |
   | FILE_NAME          | CHARACTER(256)  | File Name             |
   | FILE_SIZE          | NUMERIC         | File size in Bytes    |
   | LAST_MODIFIED_DTTM | NUMERIC         | Last modified date    |

   \ingroup utilities
   \author  SAS Institute Inc.
   \date    2015
*/
%macro rsk_dir_list(directory =
                     , ds_out = TMP_DIR_CONTENT
                     , recursive = Y
                     , filemask =
                     , regexp = N
                     , case_sensitive = N
                     , include_folders = N
                     );

   %local
      PATTERN
      APPLY_PATTERN
      FINISHED
      CURRENT_DIRECTORY
      i
   ;

   /* Make sure the parameter include_folders is specified */
   %if %sysevalf(%superq(include_folders) =, boolean) %then
      %let include_folders = N;
   %else
      %let include_folders = %upcase(&include_folders.);

   %if(%sysfunc(exist(&DS_OUT.))) %then %do;
      /* Drop output table */
      proc sql;
         drop table &DS_OUT;
      quit;
   %end;

   /* Process Filemask */
   %if(%length(%cmpres(&FILEMASK.)) > 0) %then %do;
      %let APPLY_PATTERN = YES;
      %if(%upcase(&REGEXP.) EQ Y) %then %do;
         /* Use the regular expression as it is */
         %let PATTERN = &FILEMASK.;
      %end;
      %else %do;
         %if("%substr(&FILEMASK., 1, 1)" EQ "*" AND "%substr(&FILEMASK., %length(&FILEMASK.))" EQ "*") %then
            /* Remove the asterisks at both sides of the filemask */
            %let PATTERN = %substr(&FILEMASK., 2, %eval(%length(&FILEMASK.)-2));
         %else %if("%substr(&FILEMASK., 1, 1)" EQ "*") %then
            /* Remove the asterisk at the beginning of the filemask and add the $ symbol to match at the end of the string */
            %let PATTERN = %substr(&FILEMASK., 2)$;
         %else %if("%substr(&FILEMASK., %length(&FILEMASK.))" EQ "*") %then
            /* Remove the asterisk at the end of the filemask and add the ^ symbol to match at the beginning of the string */
            %let PATTERN = ^%substr(&FILEMASK., 1, %eval(%length(&FILEMASK.)-1));
         %else
            %let PATTERN = ^&FILEMASK.$;
         /* Convert the filemask by replacing the asterisks with an equivalent regular expression */
         /*%let PATTERN = %sysfunc(tranwrd(&PATTERN., *, (\s)*(\w)*(\s)*))*/
         %let PATTERN = %sysfunc(tranwrd(&PATTERN., *, (.*)));
      %end;
   %end;
   %else
      %let APPLY_PATTERN = NO;


   %let i = 1;
   %do %while("%cmpres(%scan(&DIRECTORY., &i.,%str(,)))" NE "");

      %let CURRENT_DIRECTORY = %scan(&DIRECTORY., &i.,%str(,));
      %let FINISHED = FALSE;
      %do %while (&FINISHED. NE TRUE);

         %put NOTE: --------------------------------------------------------------;
         %put NOTE: Processing Directory: &CURRENT_DIRECTORY.;
         %put NOTE: --------------------------------------------------------------;

         data _TMP_FILE (where = (ITEM_TYPE EQ "File"))
             _TMP_DIR (where = (ITEM_TYPE EQ "Directory"))
            ;

            length
               PARENT_DIR $2000.
               ITEM_TYPE  $10.
               FILE_PATH $2000.
               FILE_NAME $256.
               FILE_SIZE 8.
               LAST_MODIFIED_DTTM 8.
            ;

            format
               FILE_SIZE comma25.
               LAST_MODIFIED_DTTM datetime21.
            ;
            keep
               PARENT_DIR
               ITEM_TYPE
               FILE_PATH
               FILE_NAME
               FILE_SIZE
               LAST_MODIFIED_DTTM
            ;

            PARENT_DIR = "&CURRENT_DIRECTORY.";
            /* Set the directory filename */
            rc = filename("fid", PARENT_DIR);
            if(rc EQ 0) then do;
               /* Open the directory */
               did = dopen('fid');
               if(did NE 0) then do;
                  /* Count the number of items inside the directory */
                  item_cnt = dnum(did);
                  if (item_cnt > 0) then do;
                     /* Loop through the items */
                     do i = 1 to item_cnt;
                        FILE_NAME = dread(did, i);
                        FILE_PATH = cats(PARENT_DIR, "/", FILE_NAME);
                        /* Check if this is a file or a directory */
                        curr_rc = filename("tmp_fid", FILE_PATH);

                        curr_did = dopen("tmp_fid");

                        if(curr_did > 0) then do;
                           /* It is a Directory */
                           ITEM_TYPE = "Directory";
                           d_rc = dclose(curr_did);
                        end;
                        else do;
                           /* It is a file */
                           ITEM_TYPE = "File";
                           /* Open the fileref */
                           curr_fid = fopen("tmp_fid");
                           /* Get file infos (size and last modified dttm) */
                           if curr_fid > 0 then do;
                              FILE_SIZE = finfo(curr_fid,'File Size (bytes)');
                              LAST_MODIFIED_DTTM = input(finfo(curr_fid,'Last Modified'), ANYDTDTM21.);
                              f_rc = fclose(curr_fid);
                           end;
                        end;

                        %if(&APPLY_PATTERN. EQ YES) %then
                           %if(&CASE_SENSITIVE. EQ Y) %then
                              if(ITEM_TYPE EQ "Directory" OR prxmatch("/&PATTERN./", strip(FILE_NAME)) > 0) then;
                           %else
                              if(ITEM_TYPE EQ "Directory" OR prxmatch("%lowcase(/&PATTERN./)", strip(lowcase(FILE_NAME))) > 0) then;

                        output;
                     end;
                  end;
               end;
            end;
            else do;
               msg = sysmsg();
               put msg;
            end;
         run;

         /* Append processed files to the output table */
         proc append base = &DS_OUT.
                  data = _TMP_FILE force;
         run;

         %if (&include_folders. = Y) %then %do;
            /* Append processed folders to the output table */
            proc append base = &DS_OUT.
                     data = _TMP_DIR force;
            run;
         %end;

         /* Add the new directories to the list */
         proc append base = _TMP_DIR_LIST
                  data = _TMP_DIR force;
         run;
         proc sort data = _TMP_DIR_LIST nodupkey;
            by FILE_PATH;
         run;

         /* Cleanup */
         proc datasets library = work nolist nodetails;
            delete
               _TMP_FILE
               _TMP_DIR
            ;
         quit;

         /* Set the macro variable that controls the loop to TRUE -> this will break the loop */
         %let FINISHED = TRUE;

         %if(&RECURSIVE. EQ Y) %then %do;
            /* Check the list of directories that have still to be processed */
            data _TMP_DIR_LIST;
               set _TMP_DIR_LIST;
               /* Read the first entry (if any) */
               if _N_ EQ 1 then do;
                  /* There is at least one directory to process. Set FINISHED -> FALSE */
                  call symputx("FINISHED", "FALSE", "L");
                  /* Set the path of the directory to be processed */
                  call symputx("CURRENT_DIRECTORY", FILE_PATH, "L");
                  /* remove this entry from the list */
                  delete;
               end;
            run;
         %end;

      %end;

      /* Cleanup */
      proc datasets library = work nolist nodetails;
         delete
            _TMP_DIR_LIST
         ;
      quit;

      %let i = %eval(&i. + 1);

   %end;

%mend;
