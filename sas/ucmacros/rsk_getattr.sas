/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
\file 
\anchor rsk_getattr
\brief Returns the attributes of a given data set.

\details

\param [in]  dsName : data set name.
\param [out] attr   : data set attributes.

\n

\ingroup CommonAnalytics utilities
\author  SAS Institute Inc.
\date    2014
*/
%macro rsk_getattr(dsName,attr);
 %if &dsname. = %then %do;
    %put %nrstr ();
    %put %nrstr (RSK_GETATTR> Returns any data set attribute.);
    %put %nrstr ();
    %put %nrstr (    Syntax: %rsk_getattr (dsname, attrname););
    %put %nrstr ();
    %goto eom;
    %end;

 %local dsid attrq type attn attc;
 %let attr = %upcase (&attr.);
 %let attc = %str (CHARSET ENCRYPT ENGINE LABEL LIB MEM MODE MTYPE SORTEDBY SORTVL SORTSEQ TYPE);
 %let attn = %str (ALTERPW ANOBS ANY ARAND ARWU CRDTE ICONST INDEX ISINDEX ISSUBSET LRECL LRID MAXGEN MAXRC MODTE NDEL NEXTGEN NLOBS NLOBSF NOBS NVARS PW RADIX READPW TAPE WHSTMT WRITEPW);

 %if &attr.= or %sysfunc (index (&attc. &attn., %str( &attr. ))) = 0 %then %do;
    %put GETATTR> Invalid attribute or no attribute specified.;
    %goto eom;
 %end;

 %let dsid = %sysfunc(open(&dsName.));
 %if &dsid. > 0
    %then %do;
        %if %sysfunc (index (&attc., %str(&attr.))) ne 0
               %then %let attrq = %qsysfunc(attrc(&dsid.,&attr.));
               %else %let attrq = %sysfunc (attrn(&dsid.,&attr.));

        %if "&attr." = "LABEL" and (&attrq. = %str(%") or &attrq. = %str(%') or &attrq. = )
               %then %let attrq = %upcase (&dsName.);

        %do; %bquote(%trim (&attrq.)) %end;

        %let dsid = %sysfunc(close(&dsid.));
        %end;
    %else %put ER%str(R)OR: (rsk_getAttr) Can not open data set %upcase(&dsName.).;
%eom:
%mend rsk_getattr;
