%macro core_resolve_re_horizons(inHorizonsList =
                              , outHorizonsVar = reHorizons
                              , inScenariosCasLib =
                              , inScenariosTable =
                              , horizonVarName = horizon
                              , casSessionName =
                              );

   %local   scenCasLibref
            updatedHorizons expandedHorizons
            max_horizon
            hnum currHorizon h rangeStart rangeEnd
            ;

   /* outHorizonsVar cannot be missing. Set a default value */
   %if(%sysevalf(%superq(outHorizonsVar) =, boolean)) %then
      %let outHorizonsVar = reHorizons;

   /* Declare the output variable as global if it does not exist */
   %if(not %symexist(&outHorizonsVar.)) %then
      %global &outHorizonsVar.;

   %let scenCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&inScenariosCasLib." sessref=&casSessionName.);

   /****************************/
   /*** HORIZON LIST UPDATES ***/
   /****************************/

   %let updatedHorizons=&inHorizonsList.;

    /*
      1. Replace any occurence of MAX in the horizon list with the actual maximum horizon number.
         Or, if the horizon list is empty, set it the maximum horizon number.
      2. If the horizon list is set to ALL, update it to missing */
   %if %index(%upcase("&inHorizonsList."), MAX)>0 or "&inHorizonsList."="" %then %do;

      proc fedsql sessref=&casSessionName.;
         create table "&inScenariosCasLib.".max_horizon_table {options replace=true} as
         select max(&horizonVarName.) as max_horizon
         from "&inScenariosCasLib."."&inScenariosTable."n
         ;
      quit;

      data _null_;
         set &scenCasLibref..max_horizon_table;
         call symputx("max_horizon", max_horizon, "L");
      run;

      %let updatedHorizons=%sysfunc(coalescec(%bquote(%sysfunc(prxchange(s/MAX/&max_horizon./i, -1, %bquote(&inHorizonsList.)))), &max_horizon.));

   %end;

   /*
      1. Expand ranges:     Example: horizons=1-3, 372      --> horizonIndex=1,2,3,372
      2. Remove duplicates: Example: horizons=1,2,2,3,372   --> horizonIndex=1,2,3,372
   */
   %let expandedHorizons=;
   %do hnum=1 %to %sysfunc(countw(%bquote(&updatedHorizons.) %str(,)));

      %let currHorizon = %scan(%bquote(&updatedHorizons.), &hnum., %str(,));
      %if %index(&currHorizon., %str(-))>0 %then %do;
         %let rangeStart=%scan(%bquote(&currHorizon.), 1, %str(-));
         %let rangeEnd=%scan(%bquote(&currHorizon.), 2, %str(-));
         %do h=&rangeStart. %to &rangeEnd.;
            %if not %sysfunc(prxmatch(s/\b&h.\b//, %bquote(&expandedHorizons.))) %then
               %let expandedHorizons = &expandedHorizons.%sysfunc(ifc(&hnum. ne 1 or &h. ne &rangeStart.,%str(,),))&h.;
         %end;
      %end;
      %else %do;
         %if not %sysfunc(prxmatch(s/\b&currHorizon.\b//, %bquote(&expandedHorizons.))) %then
            %let expandedHorizons = &expandedHorizons.%sysfunc(ifc(&hnum. ne 1,%str(,),))&currHorizon.;
      %end;

   %end;

   %let &outHorizonsVar.=&expandedHorizons.;

   libname &scenCasLibref. clear;

%mend;