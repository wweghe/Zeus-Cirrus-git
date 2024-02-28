/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/*!
\file
\anchor rsk_get_unique_ref
\brief Returns a fileref that has not been already assigned

\param [in] prefix The prefix to use for assigning the Fileref or Libref. An attempt is made to first use the prefix as the fileref/libref (i.e. without appending any random suffix) if the name is available
\param [in] type Specifies whether a fileref or a libref is to be assigned. Accepted values (case insensitive): Lib/Libref/File/Fileref. (Default: file)
\param [in] path (Optional) Path to the file/directory that the fileref/libref is assigned to
\param [in] engine (Optional) The library engine (i.e. BASE, POSTGRES, META, etc.) if TYPE = Lib/Libref, or file device (i.e. TEMP, PIPE, etc.) if TYPE = File/Fileref.
\param [in] args (Optional) Additional arguments passed to the filename() or libname() functions. (See the filename and libname functions documentation for details)
\param [in] maxTry Maximum number of attempts before giving up (Default: 1000)
\param [in] debug True/False. If True, debugging info will be printed to the log. (Default: false)

\details
This macro will return a fileref that has not been already assigned in the current SAS session.
If the <i>path</i> or <i>engine</i> parameters have been specified, a filename is assigned prior to returning the fileref.

An example macro invocation is as follows:
\code
   %* Return a unique fileref without assigning it;
   %let fref = %rsk_get_unique_ref();

   %* Return a unique fileref using the prefix "test" and assign it as a TEMP filename;
   %let fref = %rsk_get_unique_ref(prefix = test, engine = temp);

   %* Return a unique fileref using the prefix "test" and assign a filename to the path /foo/bar;
   %let fref = %rsk_get_unique_ref(prefix = test, path = /foo/bar);

   %* Return a unique libref without assigning it;
   %let libref = %rsk_get_unique_ref(type = lib);

   %* Return a unique libref using the prefix "test" and assign it to WORK;
   %let libref = %rsk_get_unique_ref(prefix = test, type = lib, path = (work));

   %* Return a unique libref using the prefix "test" and assign a libname to the path /foo/bar;
   %let libref = %rsk_get_unique_ref(prefix = test, type = lib, path = /foo/bar);

   %* Return a unique libref using the prefix "test" and assign to the SAS Risk Stratum Core Data Repository metadata library;
   %let libref = %rsk_get_unique_ref(prefix = test, type = lib, engine = meta, args = liburi="SASLibrary?@Name='SAS Risk Stratum Core Data Repository'" metaout=data);

\endcode

\author  SAS Institute Inc.
\date    2020
*/
%macro rsk_get_unique_ref(prefix = tmp
                          , type = file
                          , path =
                          , engine =
                          , args =
                          , maxTry = 1000
                          , debug = false);
   %local
      outref
      tempref
      fargs
      condition
      prefixLen
      Nhex
      maxNum
      stop
      rc
      i
   ;

   /* Make sure the debug parameter is set */
   %if %sysevalf(%superq(debug) =, boolean) %then
      %let debug = false;
   %else
      %let debug = %lowcase(&debug.);

   /* Check the validity of the TYPE parameter */
   %if not %sysfunc(prxmatch(/^(file|lib)(ref)?$/i, %superq(type))) %then %do;
      %put ERROR: input parameter type = &type is not valid. Valid values are: file|fileref|lib|libref;
      %abort;
   %end;

   /* Remove the "ref" suffix */
   %let type = %sysfunc(prxchange(s/^(file|lib).*$/\L$1/i, -1, %superq(type)));

   /* Set the check condition */
   %if(&type. = file) %then %do;
      /* Condition to check if a fileref is not assigned */
      %let condition = gt;
      /* When calling the filename() function, the first parameter is the name of the macro variable that contains the fileref -> outref */
      %let tempref = outref;
   %end;
   %else %do;
      /* Condition to check if a libref is not assigned */
      %let condition = ne;
      /* When calling the libname() function, the first parameter is the actual libref -> &outref. We need to defer the macro resolution (hence the use of %nrstr()) until the outref macro variable is assigned in the loop below. */
      %let tempref = %nrstr(&outref.);
   %end;

   /* Make sure the prefix is set (this is to avoid generating a random fileref/libref which starts with a number) */
   %if %sysevalf(%superq(prefix) =, boolean) %then
      %let prefix = tmp;

   /* Make sure the prefix contains valid characters and does not start with a number */
   %if(%sysfunc(notname(&prefix.)) or %sysfunc(prxmatch(/^\d/, %superq(prefix)))) %then %do;
      %put WARNING: Input parameter prefix = &prefix. is not a valid &type.ref. Setting the value prefix = tmp;
      %let prefix = tmp;
   %end;

   /* Make sure the maxTry parameter is a positive integer */
   %if (not %sysfunc(prxmatch(/^\d+$/, %superq(maxTry)))) %then %do;
      %put WARNING: Input parameter maxTry = &maxTry. is invalid. Setting the value maxTry = 1000;
      %let maxTry = 1000;
   %end;

   /* Get the prefix length */
   %let prefixLen = %length(&prefix.);
   /* A fileref/libref can be 8 characters long. Find out the number of available characters Nhex = 8 - &prefixLen. */
   %let Nhex = %eval(8 - &prefixLen.);
   /* Make sure Nhex is not negative */
   %let Nhex = %sysfunc(max(0, &Nhex.));

   /* Set the path (if specified) */
   %if %sysevalf(%superq(path) ne, boolean) %then
      %let fargs = &path.;

   /* Set the device/engine type (if specified) */
   %if %sysevalf(%superq(engine) ne, boolean) %then
      %let fargs = &fargs., &engine.;

   /* Set the filname engine type (if specified) */
   %if %sysevalf(%superq(args) ne, boolean) %then %do;
      /* Check if both Engine and args have been specified */
      %if %sysevalf(%superq(engine) ne, boolean) %then
         /* fargs: <path>, <engine>, <args> */
         %let fargs = &fargs., &args.;
      %else
         /* fargs: <path>, , <args> */
         %let fargs = &fargs., , &args.;
   %end;

   /* Based on the number of available characters Nhex, the maximum number that can be represented using a Hex characters is 16^Nhex - 1 */
   %let maxNum = %eval(16**&Nhex. - 1);

   /* Make sure maxTry is not bigger than 2*maxNum: Filerefs/Librefs are generated using uniform random generation.
      Since the numbers are drawn with replacement, we need to increase the number of Tries if we want to have a higher coverage of the entire range.
      Setting 2*maxNum will result (on average) in 85% coverage of the full range
   */
   %let maxTry = %sysfunc(min(&maxTry., %eval(2*&maxNum.)));

   /* Initialize the loop */
   %let i = 1;
   %let stop = N;
   %let outref = &prefix.;
   /* Start looping */
   %do %while(&stop. = N and &i. <= &maxTry.);

      /* Print debug message  */
      %if(&debug. = true) %then
         %put -> Checking &type.ref &outref. (attempt &i. of &maxTry.);

      /* Check if the fileref/libref is assigned */
      %if %sysfunc(&type.ref(&outref.)) &condition. 0 %then %do;
         /* The fileref is not assigned. Check if we need to assign it */
         %if %sysevalf(%superq(fargs) ne, boolean) %then %do;
            /* Try to assign the fileref/libref */
            %let rc = %sysfunc(&type.name(%unquote(&tempref.),&fargs.));
            /* Check for errors */
            %if &rc. %then
               %put %sysfunc(sysmsg());
         %end;
         /* Return the filref and exit */
         &outref.
         %return;
      %end;

      /* Make sure Nhex is positive */
      %if(&Nhex. > 0) %then
         /* Generate a random fileref */
         %let outref = &prefix.%sysfunc(rand(uniform, 0, &maxNum.), hex&Nhex..);
      %else
         /* The prefix is already 8 characters long and we have already tested it. Exit the loop. */
         %let stop = Y;

      /* Increment the counter */
      %let i = %eval(&i. + 1);

   %end;

   /* If we got this far it means we could not find any available fileref/libref */
   %if(&Nhex. > 0) %then
      %put WARNING: Unable to find available &type.ref in range &prefix.[%sysfunc(putn(0, hex&Nhex..))-%sysfunc(putn(&maxNum., hex&Nhex..))] within the specified maximum number of attempts (maxTry = &maxTry.). Try to increase the maxTry parameter or specify a shorter prefix to expand the search range.;
   %else
      %put WARNING: The &type.ref &prefix. is not available. Specify a shorter prefix to expand the search range;

%mend;