%macro core_query_re_results(envTableCasLib =
                             , envTableName =
                             , horizons =
                             , scenarios =
                             , outputTable = VALUES
                             , filter =
                             , outputVariables =
                             , queryType =
                             , aggregations =
                             , statistics =
                             , advancedOptions =
                             , groupByVars =
                             , orderByVars =
                             , casSessionName =
                             , outCasLib =
                             , outResults =
                             , promoteResults = Y
                             , saveResults = N
                          );

   %local   envCasLibref statesCasLibref
            statesLib statesTable valuesLib valuesTable
            outHorizons quotedAggregations quotedOutputVariables quotedReStatistics
            newHorizons currHorizon rangeStart rangeEnd h i;

   %if(%sysevalf(%superq(envTableCasLib) eq, boolean)) %then %do;
      %put ERROR: envTableCasLib is required. (caslib of the pipeline environment table);
      %abort;
   %end;

   %if(%sysevalf(%superq(envTableName) eq, boolean)) %then %do;
      %put ERROR: envTableName is required. (name of the pipeline environment table);
      %abort;
   %end;

   %if(%sysevalf(%superq(orderByVars) ne, boolean)) and (%sysevalf(%superq(groupByVars) eq, boolean)) %then %do;
      %put WARNING: orderByVars is ignored because groupByVars is not set.  Ordering of results can only be done if groupByVars are specified.;
   %end;

   %let envCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&envTableCasLib." sessref=&casSessionName.);

   /* If group-by vars are requested, the table must be promoted for partitioning to occur */
   %if (%sysevalf(%superq(groupByVars) ne, boolean)) %then
      %let promoteResults=Y;
   %else %if(%sysevalf(%superq(orderByVars) ne, boolean)) %then
      %put WARNING: orderByVars was specified but groupByVars was not.  Ordering of results can only be done if groupByVars are specified.;

   %if %upcase("&horizons.") eq "MAX" %then
      %let horizons=;

   %if "&outputTable." eq "" %then
      %let outputTable=VALUES;

   /* Set queryType to either NOAGG or AGGREGATE if not provided, based on aggregations provided. */
   %if(%sysevalf(%superq(queryType) eq, boolean)) %then %do;
      %if %upcase("&aggregations.") eq "INSTID" %then
         %let queryType = NOAGG;
      %else
         %let queryType = AGGREGATE;
   %end;

   /* If queryType is NOAGG, aggregations should be set to missing to avoid RE warnings */
   %let queryType = %upcase(&queryType.);
   %if &queryType. eq NOAGGREGATE or &queryType. eq NOAGG %then %do;
      %let queryType = NOAGG;
      %let aggregations =;
   %end;
   %else %if &queryType. ne AGGREGATE %then %do;
      %put ERROR: queryType must be either AGGREGATE or NOAGG/NOAGGREGATE;
      %abort;
   %end;

   /* When creating RE pipelines using Cirrus REST macros, the reAggregationLevels macrovariable does a top level query
   if it is _TOP_LEVEL_.  The riskresults.query CAS action just needs no aggregation levels specified to do a top level query,
   so set aggregations to missing in case we get _TOP_LEVEL_  */
   %if %upcase("&aggregations.") eq "_TOP_LEVEL_" %then
      %let aggregations=;

   %let quotedAggregations = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&aggregations)));

   /* Load the pipeline's environment table into this CAS session
         -If the pipeline's env table isn't available in-memory, load it from disk.
         -If the pipeline's env table is available in-memory, create a view with a DATA step friendly name to it
   */
   /* Note: this requires that the pipeline promoted or saved the risk environment */
   %rsk_dsexist_cas(cas_lib=%superq(envTableCasLib),cas_table=%superq(envTableName), cas_session_name=&casSessionName.);
   %if not &cas_table_exists. %then %do;
      proc cas;
         session &casSessionName.;
         table.loadTable status=rc /
            caslib="&envTableCasLib."
            path="&envTableName..sashdat"
            casOut={caslib="&envTableCasLib." name="re_env_table" replace=TRUE}
         ;
         symputx("rc_load", rc.severity, "L"); /* rc.severity=0 if successful load */
         run;
      quit;

      /* Verify that the environment table is now in-memory for this session.  If it still isn't, error out. */
      %rsk_dsexist_cas(cas_lib=%superq(envTableCasLib),cas_table=%superq(envTableName), cas_session_name=&casSessionName.);
      %if &rc_load. or not &cas_table_exists. %then %do;
         %put ERROR: Failed to find or load the pipeline environment table: &envTableCasLib..&envTableName.;
         %abort;
      %end;

   %end;
   %else %do;
      proc cas;
         session &casSessionName.;
         table.view /
            caslib="&envTableCasLib." name="re_env_table" replace=TRUE
            tables = { { caslib="&envTableCasLib." name="&envTableName." } }
         ;
      run;
   %end;

   data _null_;
      set &envCasLibref..re_env_table;
      if category="STATES_TABLE" and subcategory="SCENARIOS" then do;
         call symputx("statesLib", scan(tableref, 1, '.'), "L");
         call symputx("statesTable", scan(tableref, 2, '.'), "L");
      end;
      else if category="VALUES_TABLE" and subcategory="SCENARIOS" then do;
         call symputx("valuesLib", scan(tableref, 1, '.'), "L");
         call symputx("valuesTable", scan(tableref, 2, '.'), "L");
      end;
      else if category="PIPELINE" then
         call symputx("pipelineName", name, "L");
   run;

   /* Load the pipeline's scenStates table for the query, if not already in-memory */
   %rsk_dsexist_cas(cas_lib=%superq(statesLib),cas_table=%superq(statesTable), cas_session_name=&casSessionName.);
   %if not &cas_table_exists. %then %do;
      proc cas;
         session &casSessionName.;
         table.loadTable status=rc /
            caslib="&statesLib."
            path="&statesTable..sashdat"
            casOut={caslib="&statesLib." name="&statesTable." replace=TRUE}
         ;
         symputx("rc_load", rc.severity, "L"); /* rc.severity=0 if successful load */
         run;
      quit;

      /* Verify that the states table is now in-memory for this session.  If it still isn't, error out. */
      %rsk_dsexist_cas(cas_lib=%superq(statesLib),cas_table=%superq(statesTable), cas_session_name=&casSessionName.);
      %if &rc_load. or not &cas_table_exists. %then %do;
         %put ERROR: Failed to find or load the pipeline states table: &statesLib..&statesTable.;
         %abort;
      %end;

   %end;

   /* Load the pipeline's values table for the query, if not already in-memory */
   %rsk_dsexist_cas(cas_lib=%superq(valuesLib),cas_table=%superq(valuesTable), cas_session_name=&casSessionName.);
   %if not &cas_table_exists. %then %do;
      proc cas;
         session &casSessionName.;
         table.loadTable status=rc /
            caslib="&valuesLib."
            path="&valuesTable..sashdat"
            casOut={caslib="&valuesLib." name="&valuesTable." replace=TRUE}
         ;
         symputx("rc_load", rc.severity, "L"); /* rc.severity=0 if successful load */
         run;
      quit;

      /* Verify that the values table is now in-memory for this session.  If it still isn't, error out. */
      %rsk_dsexist_cas(cas_lib=%superq(valuesLib),cas_table=%superq(valuesTable), cas_session_name=&casSessionName.);
      %if &rc_load. or not &cas_table_exists. %then %do;
         %put ERROR: Failed to find or load the pipeline values table: &valuesLib..&valuesTable.;
         %abort;
      %end;

   %end;

   /* Update the horizons var to be what RE requires to avoid errors:
      1. Expand ranges:     Example: horizons=1-3, 372      --> horizonIndex=1,2,3,372
      2. Remove duplicates: Example: horizons=1,2,2,3,372   --> horizonIndex=1,2,3,372
   */
   %let newHorizons=;
   %do i=1 %to %sysfunc(countw(%bquote(&horizons.) %str(,)));

      %let currHorizon = %scan(%bquote(&horizons.), &i., %str(,));

      %if %index(&currHorizon., %str(-))>0 %then %do;
         %let rangeStart=%scan(%bquote(&currHorizon.), 1, %str(-));
         %let rangeEnd=%scan(%bquote(&currHorizon.), 2, %str(-));
         %do h=&rangeStart. %to &rangeEnd.;
            %if not %sysfunc(prxmatch(s/\b&h.\b//, %bquote(&newHorizons.))) %then
               %let newHorizons = &newHorizons.,&h.;
         %end;
      %end;
      %else %do;
         %if not %sysfunc(prxmatch(s/\b&currHorizon.\b//, %bquote(&newHorizons.))) %then
            %let newHorizons = &newHorizons.,&currHorizon.;
      %end;

   %end;
   %let horizons = %sysfunc(prxchange(s/^%str(,)//, -1, %bquote(&newHorizons.)));

   /* build the outHorizons parameter for the riskresults.query, if necessary */
   /* only needs done if we aren't using all horizons or we aren't using all scenarios */
   %let outHorizons=;
   %if "&horizons." ne "" or "&scenarios" ne "" %then %do;

      /* Determine the horizon index query */
      %let horizonIndex = {", max(_horizon_), "};
      %if "&horizons." ne "" %then %do;
         %let horizonIndex = { &horizons. };
      %end;

      /* create a view to the scen states table that has a DATA-step friendly name */
      proc cas;
         session &casSessionName.;
         table.view /
            caslib="&statesLib." name="states_table" replace=TRUE
            tables = { { caslib="&statesLib." name="&statesTable." } }
         ;
      run;

      %let statesCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&statesLib." sessref=&casSessionName.);
      proc sql noprint;
         select distinct cats("{scenarioName='", scenario_name, "', horizonIndex=&horizonIndex.}") into :outHorizons separated by ', '
         from &statesCasLibref..states_table
         %if "&horizons." eq "" %then %do;
            group by scenario_name;
         %end;
         %if "&scenarios" ne "" %then %do;
            %let quoted_scenario_list = %sysfunc(prxchange(s/([^,]+)/"$1"/i, -1, %bquote(&scenarios)));
            where scenario_name in (&quoted_scenario_list.);
         %end;
      quit;

   %end;

   /* If no output variables are provided, get them all from the risk environment table */
   %if "&outputVariables." = "" %then %do;

      proc sql noprint;
         select distinct name into :outputVariables separated by ','
         from &envCasLibref..re_env_table
         where category="VARIABLE" and subcategory="PRICE"
         ;
      quit;

   %end;
   %let quotedOutputVariables = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&outputVariables)));

   /* run the riskresults.query action to create the output results table */
   proc cas;
      session &casSessionName.;
      action riskresults.query /
         envTable={
            caslib="&envTableCasLib."
            name="re_env_table"
         }
         type="&queryType."
         outputs={
            {
               type="&outputTable."
               table={
                  caslib="&outCasLib."
                  %if "&promoteResults." eq "Y" %then %do;
                     name="_tmp_query_results"
                  %end;
                  %else %do;
                     name="&outResults."
                  %end;
               }
            }
         }
         outHorizons = { &outHorizons. }
         outVars = {
            keep = {&quotedOutputVariables.}
         }
         %if "&filter." ne "" %then %do;
            filters = {
               {
                  where = "&filter."
               }
            }
         %end;
         %if "&aggregations." ne "" %then %do;
            requests={
               {
                  levels={&quotedAggregations.}
               }
            }
         %end;
         %if "&statistics." ne "" %then %do;
            %let quotedReStatistics = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&statistics)));
            statistics = {
               {
                  keep = {"&quotedReStatistics."}
               }
            }
         %end;
         riskLog = {
            traceOut = {
               caslib = "&outCasLib.",
               name = "&pipelineName._QMethLog"
            }
         }
         %if(%sysevalf(%superq(advancedOptions) ne, boolean)) %then %do;
            %do i=1 %to %sysfunc(countw(%superq(advancedOptions), '|'));
               %scan(%superq(advancedOptions), &i., |);
            %end;
         %end;
         ;
      run;
   quit;

   /* If we're promoting results, drop the results table if it already exists */
   %if "&promoteResults." eq "Y" %then %do;
      proc cas;
         session &casSessionName.;
         table.droptable / caslib="&outCasLib." name="&outResults." quiet=TRUE;
         run;
      quit;
   %end;

   /* if requested, partition the query results. (if groupByVars specified, promoteResults=Y) */
   %if "&groupByVars." ne "" %then %do;
      proc cas;
         table.partition /
            casout={
               caslib="&outCasLib."
               name="&outResults."
               promote=TRUE
            }
            table={
               caslib="&outCasLib."
               name="_tmp_query_results"
               groupby={
                  %do i=1 %to %sysfunc(countw(&groupByVars., %str( )));
                     {name="%scan(%bquote(&groupByVars.), &i., %str( ))"}
                  %end;
               }
               orderby={
                  %do i=1 %to %sysfunc(countw(&orderByVars., %str( )));
                     {name="%scan(%bquote(&orderByVars.), &i., %str( ))"}
                  %end;
               }
            };
         run;
      quit;

      proc cas;
         session &casSessionName.;
         table.droptable / caslib="&outCasLib." name="_tmp_query_results" quiet=TRUE;
         run;
      quit;

   %end;

   /* if requested, promote the table to global scope */
   %else %if "&promoteResults." = "Y" %then %do;

      proc casutil;
           promote inCaslib="&outCasLib."  casData="_tmp_query_results"
                   outCaslib="&outCasLib." casOut="&outResults.";
      run;

   %end;

   /* if requested, save the table to disk in the caslib path */
   %if "&saveResults." = "Y" %then %do;

      proc cas;
          session &casSessionName.;

          table.tableExists result=r1 /  caslib="&outCasLib." name="&outResults.";
          if (r1.exists != 0) then do;
              table.save /
                  table = {caslib="&outCasLib.", name="&outResults."}
                  caslib="&outCasLib."  name ="&outResults..sashdat" replace = True
              ;
          end;

         run;
      quit;

   %end;

   libname &envCasLibref. clear;
   libname &statesCasLibref. clear;

%mend;
