/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/*!
\file
\anchor rsk_validation_funcs
\brief   Define data validation functions

\details

This macro defines a set of data validation functions.

At the moment five kinds of validating functions are defined:

- isValidKey*(x)  - tests whether string x satisfies a neccessary condition to be a valid UUID key,
                    i.e. is [a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}
- isValidOrdN*(n) - tests whether number n is a valid SAS ordinal, i.e. is a not negative integer
                    lees or equal 9007199254740991 (the maximal fixed position integer). This accepts
                    strings representing such numbers in contexts when 4GL auto-casting occurs.
- isValidOrdC*(s) - tests whether string s represents a number according to isValidOrdN.
- isValidId*(s)   - tests whether string is a valid id in a form that is accepted as an id by the UI,
                    i.e. is [a-zA-Z0-9]+([a-zA-Z0-9._-])* of length <= 50.
- isValidBool*(b) - tests whether string b is equal to "true" or "false" case insensitive.

All the functions come in two versions: as inline code that can be used in data steps (* = Inline)
and macro functions that can be used in macro code (* =).

Example:

\code

data _null_;
	x = %isValidKeyInline("12345678-1234-1234-1234-123456789012"); put x=;
	y = %isValidOrdNInline(1234); put y=;
	z = %isValidOrdCInline("1234"); put z=;
	a = %isValidIdInline("A_valid_ID"); put a=;
	b = %isValidBoolInline("true"); put b=;
run;	

%put %isValidKey(12345678-1234-1234-1234-123456789012);
%put %isValidOrdN(1234);
%put %isValidOrdC(1234);
%put %isValidId(A_valid_ID);
%put %isValidBool(true);

\endcode

\author  SAS Institute Inc.
\date    2023
*/


%macro rsk_validation_funcs;
	%put INIT: rsk_validation_funcs;
%mend;

%macro isValidKeyInline(str);
   (prxmatch('/^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}[\s]*$/', &str.))
%mend;
%macro isValidKey(str);
   %if %sysfunc(prxmatch(/^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}[\s]*$/, %superq(str))) %then 1;
   %else 0;
%mend;

%macro isValidIdInline(str);
   (prxmatch('/^[a-zA-Z0-9]+([a-zA-Z0-9._-])*$/', &str.) and length(&str.) <= 50)
%mend;
%macro isValidId(str);
   %if %sysevalf(%sysfunc(prxmatch(/^[a-zA-Z0-9]+([a-zA-Z0-9._-])*$/, %superq(str))) and %length(%superq(str)) <= 50, boolean) %then 1;
   %else 0;
%mend;

%macro isValidOrdNInline(n);
   (&n. >= 0 and floor(&n.)=&n. and &n. <= 9007199254740991)
%mend;
%macro isValidOrdN(n);
   %if %sysevalf(%superq(n) >= 0 and %sysfunc(floor(%superq(n)))=%superq(n) and %superq(n) <= 9007199254740991, boolean) %then 1;
   %else 0;
%mend;

%macro isValidOrdCInline(str);
   (0 <= input(&str., ?? best32.) <= 9007199254740991)
%mend;
%macro isValidOrdC(str);
   %if %sysfunc(prxmatch(/^0$|^[1-9][0-9]*$/, %superq(str))) %then %do;
      %if %sysfunc(inputn(%superq(str), best32.)) <= 9007199254740991 %then 1;
      %else 0;      
   %end;
   %else 0;
%mend;

%macro isValidBoolInline(b);
   (prxmatch('/^(?i)[\s]*(true|false)[\s]*$/', &b.))
%mend;
%macro isValidBool(b);
   %if %sysfunc(prxmatch(/^(?i)[\s]*(true|false)[\s]*$/, %superq(b))) %then 1;
   %else 0;
%mend;
