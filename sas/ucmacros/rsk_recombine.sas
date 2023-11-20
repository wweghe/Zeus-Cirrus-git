/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_recombine
\brief The macro rsk_recombine.sas stacks the set of partitioned tables into one non-partitioned table.

\details

<b> Identified Inputs </b>

The following inputs are required by this macro:
  \param[in] IN_DS_NM             The table name of the input partitioned datasets
  \param[in] LIB_PREFIX           The library prefix of the input libraries when appending partitioned tables
  \param[in] TECHNIQUE            Technique to use - has performance implications (SET/APPEND)
  \param[in] PARTITON_NO          Number of partitions

<b> Identified Output</b>

  \param[out]   O_DS             Output non-partitioned table

  The table O_DS has the same structure as the input table IN_DS_NM

\author SAS Institute INC.
\date 2015

 */


%macro rsk_recombine(IN_DS_NM = ,
                     LIB_PREFIX = ,
                     PARTITION_NO = ,
                     TECHNIQUE = SET,
                     O_DS = );

  %local i;
  /*strip libname from dataset*/
  %let DS=%scan(&IN_DS_NM, 2, '.');

   /* Stack partitioned datasets into one. */

   %if &TECHNIQUE eq SET %then %do;

      data &O_DS;

           set
            %do i=1 %to &PARTITION_NO;
                   &LIB_PREFIX.&i..&DS.
            %end;;

      run;

   %end;
   %else %if &TECHNIQUE eq APPEND %then %do;

      %do i=1 %to &PARTITION_NO;
         proc append base=&O_DS data = &LIB_PREFIX.&i..&DS. force;
      %end;
      run;

   %end;

%mend;
