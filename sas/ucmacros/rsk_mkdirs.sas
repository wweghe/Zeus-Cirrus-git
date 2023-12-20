/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file
\anchor rsk_mkdirs
\brief Creates the directory corresponding to a given pathname, including any intermediate non-existent parent directory.

\details

\param [in]  DIR : relative or absolute pathname.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_mkdirs(DIR);

   %local lastchar child parent;

   %*------------------------------------------------------------------;
   %* Examine the last character of the pathname.  If it is a colon,   ;
   %* then the path is an unadorned drive letter like c: or d:         ;
   %* Do nothing in that case - the drive is assumed to exists for now ;
   %*------------------------------------------------------------------;

   %let lastchar = %substr(&DIR, %length(&DIR));
   %if (%bquote(&lastchar) eq %str(:)) %then
      %return;

   %*------------------------------------------------------------------;
   %* Check whether the last character is a path separator, either the ;
   %* Unix or Windows version                                          ;
   %*------------------------------------------------------------------;

   %if (%bquote(&lastchar) eq %str(/)) or
       (%bquote(&lastchar) eq %str(\)) %then %do;

      %*---------------------------------------------------------------;
      %* If the whole path consists only of this path separator, then  ;
      %* the path implies the root directory of the current drive,     ;
      %* which is assumed to exist.  Nothing further needs to be done  ;
      %* with it.                                                      ;
      %*---------------------------------------------------------------;

      %if (%length(&DIR) eq 1) %then
         %return;

      %*---------------------------------------------------------------;
      %* Otherwise, strip off the final path separator so that the     ;
      %* path looks like this:                                         ;
      %*                                                               ;
      %*       /something/parent/child                                 ;
      %*                                                               ;
      %* instead of this:                                              ;
      %*                                                               ;
      %*       /something/parent/child/                                ;
      %*---------------------------------------------------------------;

      %let DIR = %substr(&DIR, 1, %length(&DIR)-1);

   %end;

   %*------------------------------------------------------------------;
   %* If the path already exists, there is nothing further to do       ;
   %*------------------------------------------------------------------;

   %if (%sysfunc(fileexist(%bquote(&DIR))) = 0) %then %do;

      %*---------------------------------------------------------------;
      %* Get the child directory name as the token after the last path ;
      %* separator character (either Windows or unix) or colon         ;
      %*---------------------------------------------------------------;

      %let child = %scan(&DIR, -1, %str(/\:));

      %*---------------------------------------------------------------;
      %* If the child directory name is the same as the whole path,    ;
      %* then there are no parent directories to create.  Otherwise,   ;
      %* extract the parent directory name and call this macro         ;
      %* recursively to create the parent directory. If it already     ;
      %* exists, the macro call will simply return.                    ;
      %*---------------------------------------------------------------;

      %if (%length(&DIR) gt %length(&child)) %then %do;
         %let parent = %substr(&DIR, 1, %length(&DIR)-%length(&child));
         %rsk_mkdirs(&parent);
      %end;

      %*---------------------------------------------------------------;
      %* Now create the child directory in the parent.  If the         ;
      %* directory is not created for some reason, exit with an        ;
      %* error message                                                 ;
      %*---------------------------------------------------------------;

      %let dname = %sysfunc(dcreate(&child, &parent));
      %if (%bquote(&dname) eq ) %then %do;
          %put ERROR: Unable to create [&child] in [&parent] directory.;
         %abort;
      %end;
   %end;
%mend rsk_mkdirs;
