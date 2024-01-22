/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_dirtree_walk
\brief Used to walk a file-system tree - the macro is invoked at each leaf node of the tree.

\details

\param [in]  ROOT     : file system root.
\param [in]  CALLBACK : name of the callback macro.
\param [in]  MAXDEPTH : maximum recursion depth; the default value is 1.
\param [in]  ARG      : argument.

Details on the parameter CALLBACK
---------------------------------

The parameter <b>CALLBACK</b> holds the name of the callback macro.
This macro must have the following signature:

%<callback_macro_name>(id=,type=, memname=, level=,parent=);

The default value of <b>CALLBACK</b> is rsk_walk_callback which just prints messages about each node.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_dirtree_walk(ROOT,
                        CALLBACK=rsk_walk_callback,
                        MAXDEPTH=1,
                        ARG     =);
   %let id=0;
   %rsk_dirtree_walk_p(&ROOT,
                       1,
                       &id,
                       MAXDEPTH=&MAXDEPTH,
                       CALLBACK=&CALLBACK,
                       ARG=&ARG);
%mend;


%macro rsk_walk_callback(ID=,
                         TYPE=,
                         MEMNAME=,
                         LEVEL=,
                         PARENT=,
                         CONTEXT=,
                         ARG=);

   %put inside callback : id=&ID, type=&TYPE, memname=&MEMNAME, level=&LEVEL, context=&CONTEXT, arg=&ARG;

%mend;


%macro rsk_dirtree_walk_p(ROOT,
                          LEVEL,
                          PARENTID,
                          MAXDEPTH=,
                          CALLBACK=,
                          ARG=);

   %local parentId did dnum i mid level rc memname root fref;

   /* Open the directory named in the root variable */

   %let FREF = rsk&LEVEL;

   %let RC = %sysfunc(filename(FREF, &ROOT));

   %let DID = %sysfunc(dopen(&FREF));

   %if &DID = 0 %then
      %goto theend;

   /* Iterate over all the directory entries */

   %let DNUM = %sysfunc(dnum(&DID));
   %do i = 1 %to &DNUM;
      %local isDirectory;
      %let isDirectory = 0;
      %let MEMNAME = %sysfunc(dread(&DID, &i));
      /* verify that this is a directory */
      %let RC = %sysfunc(filename(FREF_T, &ROOT/&MEMNAME));
      /* try opening as a directory */
      %local DID2;
      %let DID2=%sysfunc(dopen(&FREF_T));
      %if &DID2 ne 0 %then %do;
           %let isDirectory = 1;
           %let RC = %sysfunc(dclose(&DID2));
      %end;
      %let RC = %sysfunc(filename(&FREF_T));

      %if &isDirectory %then %do;
         %let ID = %eval(&ID+1);


         /* If we have not reached the limits of recursion specified,
            invoke the macro on this subdirectory */

         %if &MAXDEPTH = &LEVEL %then %do;
         %end;
         %else %do;
            %rsk_dirtree_walk_p(&ROOT/&MEMNAME,
                                %eval(&LEVEL+1),
                                &ID,
                                MAXDEPTH=&MAXDEPTH,
                                CALLBACK=&CALLBACK,
                                ARG=&ARG);
         %end;

         %if &CALLBACK ne %then %do;
             %&CALLBACK(ID=&ID,
                        TYPE=D,
                        MEMNAME=&MEMNAME,
                        LEVEL=&LEVEL,
                        PARENT=&PARENTID,
                        CONTEXT=&ROOT,
                        ARG=&ARG);
         %end;
         %else %do;
            %put parent=&PARENTID, member=&MEMNAME, level=&LEVEL, type=D;
         %end;

      %end;
      %else %do;
         %if &CALLBACK ne %then %do;
             %&CALLBACK(ID=.,
                        TYPE=F,
                        MEMNAME=&MEMNAME,
                        LEVEL=&LEVEL,
                        PARENT=&PARENTID,
                        CONTEXT=&ROOT,
                        ARG=&ARG);
         %end;
         %else %do;
            %put parent=&PARENTID, member=&MEMNAME, level=&LEVEL, type=F;
         %end;
      %end;
   %end;

   /* Close the directory */

   %let RC = %sysfunc(dclose(&DID));

%theend:

  /* Release the file reference */

   %let RC = %sysfunc(filename(&FREF));
%mend;
