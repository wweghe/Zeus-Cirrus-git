%macro rsk_get_dtm_formats(type =, separator = |);
   %local
      fmt_list
   ;
   %if %sysevalf(%superq(type) =, boolean) %then %do;
      %put ERROR: Input parameter TYPE is missing. Expected values: DATE/DATETIME/TIME;
      %abort;
   %end;
   %else
      %let type = %upcase(&type.);

   %if %sysevalf(%superq(separator) =, boolean) %then
      %let separator = %str( );

   %if(&type. = DATE) %then
      %let fmt_list = B8601DA DATE DAY DDMMYY DOWNAME E8601DA E8601DN JULDAY JULIAN MMDDYY MMYY MONNAME MONTH MONYY NENGO NLDATE PDJUL QTR WEEK WORDDAT YEAR YYMM YYMON YYQ YYWEEK;
   %else %if(&type. = DATETIME) %then
      %let fmt_list = B8601DN B8601DT B8601DX B8601DZ B8601LX DATEAMPM DATETIME DTDATE DTMONYY DTWKDATX DTYEAR DTYYQC E8601DT E8601DX E8601DZ E8601LX MDYAMPM NLDATM;
   %else %if(&type. = TIME) %then
      %let fmt_list = B8601LZ B8601T E8601LZ E8601T HHMM HOUR MMSS NLTIM TIME TOD;
   %else %do;
      %put ERROR: Invalid value TYPE = &type.. Expected values: DATE/DATETIME/TIME;
      %abort;
   %end;

   %sysfunc(prxchange(s/\s+/%superq(separator)/i, -1, &fmt_list.))
%mend;