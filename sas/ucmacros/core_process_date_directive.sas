/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

%macro core_process_date_directive(name =, date =, align = SAME, format = yymmddn8.);
   %local
      match_str
      replace_str_1
      time_opt
      replace_str_2
      align_str
      format_str
   ;

   /* Check if the DATE parameter is missing */
   %if(%sysevalf(%superq(date) =, boolean)) %then %do;
      /* Check if the BASE_DT parameter is avaiable */
      %if (%symexist(base_dt)) %then
         /* Use the BASE_DTTM parameter */
        %let date = &base_dt.;
      %else 
         /* Use the current date */
         %let date = "%sysfunc(date(), date9.)"d;
   %end;

   /* Match one of the following cases
      - <MONTH, -1>
      - <MONTH, -1, same>
      - <MONTH, -1, same, yymmddp10.>
      - <MONTH, -1,     , yymmddp10.>
      - [MONTH, -1]
      - [MONTH, -1, same]
      - [MONTH, -1, same, yymmddp10.]
      - [MONTH, -1,     , yymmddp10.]
   */
   %let match_str = %bquote((<|\[)\s*(\w+)\s*,\s*([-+]?\d+)\s*(,\s*(\w*)\s*(,\s*(\w+\.\d*)\s*)?)?(>|\]));
   /* Check if we have a match inside name */
   %if(%sysfunc(prxmatch(/&match_str./i, %superq(name)))) %then %do;
      /* Extract the align portion (if specified) */
      %let align_str = %sysfunc(prxchange(s/^.*&match_str..*$/$5/i, -1, %superq(name)));
      %if(%sysevalf(%superq(align_str) ne, boolean)) %then
         /* Override the format parameter */
         %let align = &align_str.;

      /* Extract the format portion (if specified) */
      %let format_str = %sysfunc(prxchange(s/^.*&match_str..*$/$7/i, -1, %superq(name)));
      %if(%sysevalf(%superq(format_str) ne, boolean)) %then
         /* Override the format parameter */
         %let format = &format_str.;

      /* Replace it with the string: MONTH, &sst_as_of_dt, -1, SAME  */
      %let replace_str_1 = %bquote($2, &date., $3, &align.);

      /* Perform the string replacement */
      %let time_opt = %sysfunc(prxchange(s/^.*&match_str..*$/&replace_str_1./i, -1, %superq(name)));
      /* Compute the new date */
      %let replace_str_2 = %sysfunc(intnx(%unquote(&time_opt.)), &format.);

      /* Apply lowcase/upcase depending on the case of the format */
      %if(&format. = %lowcase(&format.)) %then
         %let replace_str_2 = %lowcase(&replace_str_2.);
      %else %if(&format. = %upcase(&format.)) %then
         %let replace_str_2 = %upcase(&replace_str_2.);

      /* Update the macro variable name, by replacing the text <MONTH, -1> with the corresponding date */
      %let name = %sysfunc(prxchange(s/&match_str./&replace_str_2./i, -1, %superq(name)));
   %end;

   /* Return the updated name */
   &name.
%mend core_process_date_directive;