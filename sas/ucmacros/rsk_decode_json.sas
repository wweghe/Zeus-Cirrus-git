/*takes a column from a SAS dataset that contains encoded JSON and decodes it by writing it to a file*/ 
/*
   dset - the SAS dataset containing the JSON column to decodes
   column - the column name containing the encoded JSON
   outfileref (optional) - fileref to the file where the decoded JSON is written
   outlibref (optional) - JSON engine libref to the decoded JSON
*/

/*
   Example that converts a REST request's resp.json file to SAS datasets, and then decodes on of the columns in those SAS datasets (the ruleData custom field)
   filename resp "/tmp/resp.json";
   libname root_lib json fileref=resp NOALLDATA NRM;
   %rsk_decode_json(table=root_lib.items_customfields, column=ruleData, outlibref=rule_lib);   
*/
   
%macro rsk_decode_json(dset=, column=, outfileref=, outlibref=);
      
      %let clear_file_ref=N;
      %if %sysevalf(%superq(outfileref) eq, boolean) %then %do;
         %let outfileref=__out__;
         %let clear_file_ref=Y;
      %end;
      %if %sysfunc(fileref(&outfileref.)) ne 0 %then %do;
         filename _tmp_ temp;
      %end;

      data _null_;
         set &dset. (keep=&column.) end=last;
         file &outfileref.;
         if _n_=1 then put "[";
         if strip(&column.) ne "" then put &column.;
         else put "{}";
         if not last then put ",";
         else put "]";
      run;
      
      %if %sysevalf(%superq(outlibref) ne, boolean) %then %do;
         libname &outlibref. json fileref=&outfileref. NOALLDATA NRM;
      %end;
      
      %if &clear_file_ref.=Y %then %do;
         filename &outfileref.;
      %end;
      
%mend;


