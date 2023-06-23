%macro rsk_get_lib_engine(libref);
   %local
      dsid
      engnum
      engine
      rc
   ;
   /* Open SASHELP.VLIBNAME dataset */
   %let dsid = %sysfunc(open(sashelp.vlibnam(where = (libname = "%upcase(&libref.)")), i));
   /* Check that it was successfully opened */
   %if (&dsid. ^= 0) %then %do;
      /* Get the column numnber for the ENGINE variable */
      %let engnum = %sysfunc(varnum(&dsid., ENGINE));
      /* Fetch first record */
      %let rc = %sysfunc(fetch(&dsid.));
      /* Get the value for the ENGINE variable */
      %let engine = %sysfunc(getvarc(&dsid., &engnum.));
      /* Close the dataset */
      %let rc = %sysfunc(close(&dsid.));
   %end;

   /* Return the engine */
   &engine.
%mend;