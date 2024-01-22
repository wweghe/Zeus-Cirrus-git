/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/*!
\file 
\anchor rsk_append_many
\brief   Collect a list of strings and files from a dataset and append them all into one file

\param [in] file fileref or full file path of the file that will be written to.
\param [in] append a SAS dataset with the items to append. See details.
\param [in] overwrite (Optional) Y or N. If Y, base file is not appended to but overwritten.
\param [in] newline (Optional) Newline character to use - default is Windows style (CRLF)

\details

The SAS dataset specified in the 'append' parameter should have the following columns, both of then character type:
   type    - Determines how value is interpreted
   value   - A string with contents to append

type can be one of:
   filepath  - Value is assumed to be a valid filepath and contents are read from filepath
   fileref   - Value is assumed to be a valie fileref
   string    - Value is appended as-is after calling strip. To add leading or trailing space, use type=space
   newline   - Value is stripped and then newline character is appended. if value is blank then just a newline character is appended. Default is CRLF.
   space     - Value is parsed as an integer and that many spaces are appended

Example:

The following code appends hardcoded text and the contents of another file to the file /temp/base.txt

\code
options mprint;
filename f1 '/temp/f1.txt';
filename base '/temp/base.txt';

data _null_;
file base;
put "Starting Contents";
run;

data _null_;
file f1;
put "New file contents";
run;


data appends;
length value $ 32000 type $ 32;

value = "START"; type="newline"; output;
value = ""; type="newline"; output;
value = "123"; type="string"; output;
value = "456"; type="newline"; output;
value = 'f1'; type="fileref"; output;
value = '<'; type="string"; output;
value = '5'; type="space"; output;
value = '>'; type="string"; output;
value = '/temp/f1.txt'; type="filepath"; output;
value = "END"; type="string"; output;
run;

%rsk_append_many(file=base,append=appends,overwrite=Y);
filename base clear;
filename f1 clear;

\endcode

The contents of /temp/base.txt are (note that the put statement adds a carriage return to the end of the string) :

\verbatim
START

123456
Some file contents
<     >Some file contents
END
\endverbatim

If instead the macro is called with overwrite=N (ommit the argument to take this as the default):

\code

%rsk_append_many(file=base,append=appends);

\endcode

The contents of /temp/base.txt are:

\verbatim

Starting Contents
START

123456
Some file contents
<     >Some file contents
END

\endverbatim

\author  SAS Institute Inc.
\date    2019
*/



%macro rsk_append_many(file =, append =, overwrite = N, newline = %str('0D0A'x));

   %local
     baseFile
     clear_file
     openMode
   ;

   %let baseFile = &file.;
   %let clear_file = N;
   %if(%length(&file.) > 8) %then %do;
      /* &file is definitely a path to a file name */
      filename _b_ "&file.";
      %let clear_file = Y;
      %let baseFile = _b_;
   %end;
   %else %do;
      %if(%sysfunc(fileref(&file.)) > 0) %then %do;
         /* It is not a fileref. Although short, it must be a file path */
         filename _b_ "&file.";
         %let clear_file = Y;
         %let baseFile = _b_;
      %end;
   %end;

   %let openMode = A;
   %if %lowcase(&overwrite.) NE n %then %do;
      %let openMode = O;
   %end;

   /* For filepath types, create filerefs. For newline types, add in carriage returns and treat as string */
   data _appendMod_;
      length fileref $ 8;
      retain newline &newline.;
      set &append.;
      if strip(lowcase(type))  eq 'filepath' then do;
         fileref = cats("_a_p",_N_);
         rc = filename(fileref, strip(value));
      end;
      else if strip(lowcase(type)) eq 'fileref' then do;
         fileref = strip(value);
      end;
      else if strip(lowcase(type)) eq 'newline' then do;
         type = 'string';
         value = cats(strip(value),strip(newline));
      end;
   run;

   /* Loop through items to append, write to base file based on type of each row */
   data _null_;
      length base 8 fileid 8;
      retain base rec space;
      set _appendMod_ end=last;
      /* Open the file we are writing to */
      if _N_ eq 1 then do;
         base = fopen("&baseFile.","&openMode.",,"B");
         rec = '20'x;
         space = rec;
      end;

      if strip(lowcase(type)) eq "string" then do;
         /* Strip leading and trailing whitespace from string and write to file */
         rc = fput(base, strip(value));
       rc =fwrite(base);
      end;
      else if substr(strip(lowcase(type)),1,4) eq "file" then do;
         /* fileref and filepath types now have a fileref so treat them the same.
            Read file one byte at a time and write to the output file */
        filein = fopen(fileref,'I',1,'B');
       do while(fread(filein)=0);
          rc = fget(filein,rec,1);
          rc = fput(base, rec);
          rc =fwrite(base);
        end;
        rc = fclose(filein);
        if substr(value,1,4) eq "_a_p" then do;
           rc = filename(fileref);
        end;
        rc = fclose(filein);
      end;
      else if strip(lowcase(type)) eq "space" then do;
         /* Convert string value to a numeric value and add that many space characters */
         num_spaces = input(strip(value), 8.);
         put num_spaces=;
         do i=1 to num_spaces;
            put "writing a space";
            rc = fput(base, space);
          rc =fwrite(base);
         end;
      end;
      else do;
         msg= cats("Unkown append type: ", type, ". Valid values are string, space, fileref and filepath.");
         put msg;
      end;
      if last then do;
         rc = fclose(base);
      end;
   run;

   /* Clear any filerefs we made earlier */
   data _null_;
      set _appendMod_;
      if strip(lowcase(type))  eq 'filepath' then do;
         rc = filename(fileref);
      end;
   run;
   data _appendMod_;
      set _null_;
   run;
   %if(&clear_file. = Y) %then %do;
      filename _b_ clear;
   %end;
%mend;