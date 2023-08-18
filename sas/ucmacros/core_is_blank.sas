/*************************************************************************
 * Copyright 2023, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
 *
 * NAME:        core_is_blank
 *
 * PURPOSE:     Check if a macro variable is blank
 *
 *
 * PARAMETERS: 
 *              mac_var 
 *                  <required> - a macro variable name
 *
 * EXAMPLE:     %core_is_blank(mac_var=test_run);
 **************************************************************************/
%macro core_is_blank(mac_var);
    %sysevalf(%superq(&mac_var)=,boolean)    
%mend core_is_blank;