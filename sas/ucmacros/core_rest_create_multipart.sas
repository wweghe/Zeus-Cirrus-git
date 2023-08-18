/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/*!
\file 
\anchor core_rest_create_multipart
\brief   Takes a json formatted string and creates a multipart/form-data request body

\param [in] body      A fileref pointing to the file that will be written to. Contents are overwritten.

\param [in] boundary  The form-boundary to use. Any unique string not expected to be found in the conetnt itself;

\param [in] content   A dataset specifying the form-data to create.  See details

\param [in] status   (optional) The name of a macro variable. Set to 1 at the begging of execution
                        and then set to 0 at the very end. Default is mpStatus.

\details

The macro creates a proper multipart request body and writes it to the file specified. The details of the request come
from the content argument. This is a dtaset with one row for each form-data element in the request. Columns are: (all are character):

   contentDisposition:    Content-disposition. example: 'name="myFile"; filename="fname.txt"'

   contentType:           The content-type value. e.g. 'text/plain','application/json', or 'application/octet-stream'

   contentFormat:         Either 'string', 'filename', or 'fileref'. Determines how the value in the 'content' column is interpreted

   content:               The actual content. If contentFormat is filename, then the contents of the file specified are read. Similarly for 'fileref'.
                          If contentFormat is 'string', then the string is written as the content verbatim.

A good general description of what multipart requests look like is here: https://swagger.io/docs/specification/describing-request-body/multipart-requests
               
Example:
\code
option mprint;
filename _input TEMP;
filename _bdy '/temp/mp.txt';
data _null_;
file _input;
put "Some text";
run;

data mpContent;
length contentDisposition contentFormat contentType content $256;
contentDisposition='name="file"; filename="someName.txt"';
contentType='application/octet-stream';
contentFormat='fileref';
content='_input';
output;
contentDisposition='name="_charset_"';
contentType='text/plain';
contentFormat='string';
content='UTF-8';
output;
run;

%core_rest_create_multipart(body=_bdy
                          ,boundary=----multpartbound271828182845
                          ,content=mpContent
                          );
filename _input clear;
filename _bdy clear;

\endcode

The contents of_bdy are:

\verbatim
------multpartbound271828182845
Content-Disposition: form-data; name="file"; filename="someName.txt"
Content-Type: application/octet-stream

Some text

------multpartbound271828182845
Content-Disposition: form-data; name="_charset_"
Content-Type: text/plain

UTF-8
------multpartbound271828182845
\endverbatim


\author  SAS Institute Inc.
\date    2019
*/

%macro core_rest_create_multipart(body=
                                ,boundary=
                                ,content=
                                ,status=mpStatus
                                );

/*Initialize status to 1 (fail);*/
   %if(not %symexist(&status.)) %then
      %global &status.;
   %let &status. = 1;

   data _pieces_;
      length type $64 value $32000;
      set &content. end=last;
      type='newline'; value="--&boundary."; output;
      type='newline'; value="Content-Disposition: form-data; " || strip(contentDisposition); output;
      type='newline'; value="Content-Type: " || strip(contentType); output;
      type='newline'; value=""; output;
      type=contentFormat; value=content; output;
      type='newline'; value=""; output;
      /* Last boundary - no CRLF after */
      if last then do;
         type='string'; value="--&boundary."; output;
      end;
   run;

   %rsk_append_many(file=&body.,append=_pieces_,overwrite=Y);

   %let &status. = 0;
%mend core_rest_create_multipart;