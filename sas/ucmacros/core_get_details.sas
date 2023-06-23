/*
 Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA
*/

/*!
\file

\brief   Retrieve data from the Data Service database

\param [in] libref Libref to the location where the detail data are stored.
\param [in] table_name Name of the table to be retrieved
\param [in] where_clause filter condition applied to the data
\param [in] custom_code Custom code to be executed as part of the data extraction. It must contain valid data step syntax.
\param [in] ds_in_aggregation_config Input configuration table that drives the data aggregation logic.
\param [in] conditional_aggregation_clause (Optional) Used when ds_in_aggregation_config has been specified. Only the records that satisfy this condition (evaluated inside an IF statement) will be aggregated
\param [in] post_aggregation_code Used when ds_in_aggregation_config has been specified. Custom code to be executed after the aggregation logic. It must contain valid data step syntax.
\param [in] aggregation_symbol Used when ds_in_aggregation_config has been specified. String used to mark variables that did not participate in the aggregation. (Default: +)
\param [in] keep_all_flg Flag: Y/N. Used when ds_in_aggregation_config has been specified. Controls whether all variables are retained in the output table or just the ones that participated in the aggregation (Default: +)
\param [in] out_type Controls the type of output being generated: DATA (physical table) or VIEW. (Default: DATA)
\param [in] out_ds_structure desired structure of output table (optional)
\param [in] dte Days to expiration. Relevant when out_type = VIEW. Number of days before the access to the data referenced by the view is revoked (Default: 365)
\param [in] pwd Password used to protect the view code. It is strongly advised to set a password, in order to prevent users from getting the credentials to the database
\param [in] logOptions Logging options (i.e. mprint mlogic symbolgen ...) to enable.
\param [out] out_ds Name of the output table/view containing the requested data/audit details

\details

Based on the provided where_clause condition, this macro will determine which partitions store the requested data and provide the requested details.
In case of fact tables, all related dimension tables are retrieved and joined with the fact table.


The structure of input configuration table <b><i>DS_IN_AGGREGATION_CONFIG</i></b> is as follows:

| Variable   | Type          | Description                                                                                                                                                                 |
|------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| VAR_NAME   | CHARACTER(32) | Name of the variable participating in the aggregation                                                                                                                     |
| VAR_SCOPE  | CHARACTER(32) | Sets the role of the variable in the aggregation: <br> - GROUP_BY <br> - COUNT <br> - DISTINCT <br> - MAX <br> - MIN <br> - MEAN <br> - SUM <br> - WSUM                   |
| WEIGHT_VAR | CHARACTER(32) | Name of the weight variable used for computing the weighted aggregation. Relevant when VAR_SCOPE = WSUM                                                                   |
| ALIAS      | CHARACTER(32) | Name given to the aggregated variable. Defaults to the value of VAR_NAME if left blank, except for the case of VAR_SCOPE = DISTINCT where the default   is UNQ_<VAR_NAME> |

This table is optional and it is used to specify how the data should be aggregated.
For example, the following configuration table:

| VAR_NAME         | VAR_SCOPE | WEIGHT_VAR | ALIAS                |
|------------------|-----------|------------|----------------------|
| country          | GROUP_BY  |            |                      |
| product_category | GROUP_BY  |            |                      |
| product_name     | COUNT     |            | sold_item_cnt        |
| product_name     | DISTINCT  |            | unique_products_cnt  |
| revenue_amt      | MEAN      |            | average_revenue_amt  |
| revenue_amt      | MAX       |            | max_revenue_amt      |
| revenue_amt      | MIN       |            | min_revenue_amt      |
| revenue_amt      | SUM       |            |                      |
| revenue_amt      | WSUM      | margin_pct | weighted_revenue_amt |

would be functionally equivalent to running the following query on the detail data:

\code
proc sql;
   select
      country
      , product_category
      , count(*) as sold_item_cnt
      , count(distinct product_name) as unique_products_cnt
      , mean(revenue_amt) as average_revenue_amt
      , max(revenue_amt) as max_revenue_amt
      , min(revenue_amt) as min_revenue_amt
      , sum(revenue_amt) as revenue_amt
      , sum(revenue_amt * margin_pct) / sum(margin_pct) as weighted_revenue_amt
   from
      <Detail Data>
   group by
      country
      , product_category
   ;
quit;
\endcode

If keep_all_flg = Y, all the other variables in the detail table that do not participate in the aggregation will be kept in the output table and assigned a missing value (if numeric) or the value specified by aggregation_symbol (if character)


\author  SAS Institute Inc.
\date    2022
*/

%macro core_get_details(source_ds_list =
                        , out_ds =
                        , lib_engine = POSTGRES
                        , source_lib_connection_options =
                        , drop_list =
                        , where_clause =
                        , custom_code =
                        , ds_in_aggregation_config =
                        , conditional_aggregation_clause =
                        , post_aggregation_code =
                        , aggregation_symbol = +
                        , keep_all_flg = Y
                        , out_type = data
                        , out_ds_structure =
                        , dte = 365
                        , pwd =
                        , logOptions =
                        )  / minoperator;


   %local
      oldLogOptions
      TotTables
      source_ds
      aggregate_flg
      conditional_aggregation_flg
      TotGroupByCnt
      TotAggrVarCnt
      TotWeightCnt
      TotUniqueCnt
      i
   ;


   /* Set the required log options */
   %if %sysevalf(%superq(logOptions) ne, boolean) %then
      options &logOptions.;
   ;

   /* Get the current value of mprint, mlogic and symbolgen options */
   %let oldLogOptions = %sysfunc(getoption(mprint)) %sysfunc(getoption(mlogic)) %sysfunc(getoption(symbolgen));

   /* Prevent the DB connection details from being printed to the log */
   option nomprint mlogic nosymbolgen;
   /*  Assign library to the Source Data */
   libname _dslib &lib_engine. %unquote(&source_lib_connection_options.);
   /* Restore log options */
   options &oldLogOptions.;

   %let TotTables = %sysfunc(countw(&source_ds_list., %str( )));
   %do i = 1 %to &TotTables.;
      %local source_ds_&i.;
      %let source_ds_&i. = %scan(&source_ds_list., &i., %str( ));
   %end;

   /* Make sure the DTE parameter has been set. */
   %if %sysevalf(%superq(dte) =, boolean) %then
      %let dte = 365;

   /* *************************************************** */
   /*       Pre-processing: Aggregation Logic             */
   /* *************************************************** */

   %let aggregate_flg = N;
   %let conditional_aggregation_flg = N;
   /* Check if we need to perform any aggregation */
   %if %sysevalf(%superq(ds_in_aggregation_config) ne, boolean) %then %do;
      /* Make sure the aggregation configuration table exists */
      %if(%rsk_dsexist(&ds_in_aggregation_config.)) %then %do;
         %let TotGroupByCnt = 0;
         %let TotAggrVarCnt = 0;
         %let TotWeightCnt = 0;
         %let TotUniqueCnt = 0;
         data _null_;
            length
               var_name $32.
               var_scope $32.
               weight_var $32.
               alias $32.
               aggr_weight_var $32.
               aggr_summary_var $32.
            ;
            set &ds_in_aggregation_config. end = last;
            retain
               group_by_cnt 0
               aggr_var_cnt 0
               wsum_cnt 0
               unique_cnt 0
            ;

            var_scope = upcase(var_scope);

            /* Declare Lookup table to keep track of all the weight variables */
            if _N_ = 1 then do;
               declare hash hWeight();
               hWeight.defineKey("weight_var");
               hWeight.defineData("aggr_weight_var");
               hWeight.defineDone();
            end;

            /* Determine the role of each variable */
            select(var_scope);
               when("GROUP_BY") do;
                  group_by_cnt + 1;
                  call symputx(cats("aggr_groupBy_var_", put(group_by_cnt, 8.)), var_name, "L");
               end;
               when("DISTINCT") do;
                  unique_cnt + 1;
                  /* Set the alias for the unique variable unless it has been specified */
                  alias = coalescec(alias, cats("UNQ_", var_name));
                  /* Variable for quich the distinct count must be computed */
                  call symputx(cats("aggr_unique_var_", put(unique_cnt, 8.)), var_name, "L");
                  call symputx(cats("aggr_unique_summary_var_", put(unique_cnt, 8.)), alias, "L");
               end;
               otherwise do;
                  aggr_var_cnt + 1;
                  /* Name of the temporary variable holding the aggregated values */
                  aggr_summary_var = cats("__summary_var_", put(aggr_var_cnt, 8.));
                  /* Set the alias for the aggregated variable unless it has been specified */
                  alias = coalescec(alias, var_name);

                  /* Variable to be aggregated */
                  call symputx(cats("aggr_var_name_", put(aggr_var_cnt, 8.)), var_name, "L");
                  call symputx(cats("aggr_summary_var_", put(aggr_var_cnt, 8.)), aggr_summary_var, "L");
                  call symputx(cats("aggr_summary_alias_", put(aggr_var_cnt, 8.)), alias, "L");

                  select(var_scope);
                     when("MAX", "MIN", "SUM") do;
                        /* Aggregation function */
                        call symputx(cats("aggr_func_", put(aggr_var_cnt, 8.)), upcase(var_scope), "L");
                        /* The expression that goes into the aggregation function. In this case it is a simple aggregation so this is the name of the variable to be aggregated */
                        call symputx(cats("aggr_expression_", put(aggr_var_cnt, 8.)), var_name, "L");
                        /* The expression to evaluate at the end of the aggregation. In this case it is a simple aggregation so this is just an assignment: VAR = <AGGREGATED VAR> */
                        call symputx(cats("aggr_final_expression_", put(aggr_var_cnt, 8.)), aggr_summary_var, "L");
                     end;
                     when("WSUM") do;
                        /* Aggregation function */
                        call symputx(cats("aggr_func_", put(aggr_var_cnt, 8.)), "SUM", "L");
                        /* The expression that goes into the aggregation function. In this case it is a weighted aggregation so this is the product of the aggregation variable by its weight */
                        call symputx(cats("aggr_expression_", put(aggr_var_cnt, 8.)), catx("*", var_name, weight_var), "L");
                        call missing(aggr_weight_var);
                        /* Check that the weight has been specified */
                        if(not missing(weight_var)) then do;
                           wsum_cnt + 1;
                           /* Check if we have already processed this weight */
                           if(hWeight.find() ne 0) then do;
                              aggr_weight_var = cats("__summary_weight_var_", put(wsum_cnt, 8.));
                              call symputx(cats("aggr_weight_var_name_", put(wsum_cnt, 8.)), weight_var, "L");
                              call symputx(cats("aggr_weight_summary_var_", put(wsum_cnt, 8.)), aggr_weight_var, "L");
                              hWeight.add();
                           end;
                        end;
                        /* The expression to evaluate at the end of the aggregation. In this case it is a weighted aggregation so we have to divide by the sum of the weight: VAR = <AGGREGATED VAR> / <SUM of WEIGHT> */
                        call symputx(cats("aggr_final_expression_", put(aggr_var_cnt, 8.)), catx("/", aggr_summary_var, aggr_weight_var), "L");
                     end;
                     when("MEAN", "AVG") do;
                        /* Aggregation function */
                        call symputx(cats("aggr_func_", put(aggr_var_cnt, 8.)), "SUM", "L");
                        call symputx(cats("aggr_expression_", put(aggr_var_cnt, 8.)), var_name, "L");
                        /* The expression to evaluate at the end of the aggregation. In this case it is a simple average so we have to divide by the row count: VAR = <AGGREGATED VAR> / <ROW COUNT> */
                        call symputx(cats("aggr_final_expression_", put(aggr_var_cnt, 8.)), catx("/", aggr_summary_var, "__row_count__"), "L");
                     end;
                     when("COUNT") do;
                        call symputx(cats("aggr_func_", put(aggr_var_cnt, 8.)), "", "L");
                        call symputx(cats("aggr_expression_", put(aggr_var_cnt, 8.)), "", "L");
                        call symputx(cats("aggr_final_expression_", put(aggr_var_cnt, 8.)), "__row_count__", "L");
                     end;
                     otherwise do;
                        put "WARNING: Unsupported aggregation function: " var_name " --> " var_scope ". This variable will not be aggregated.";
                        aggr_var_cnt = aggr_var_cnt - 1;
                     end;
                  end; /* inner select */
               end; /* otherwise (outer select) */
            end;/* outer select */

            if last then do;
               call symputx("TotGroupByCnt", group_by_cnt, "L");
               call symputx("TotAggrVarCnt", aggr_var_cnt, "L");
               call symputx("TotWeightCnt", wsum_cnt, "L");
               call symputx("TotUniqueCnt", unique_cnt, "L");
            end;

         run;

         /* Make sure we have at least one group by variable */
         %if (&TotGroupByCnt. = 0 and (&TotAggrVarCnt. > 0 or &TotUniqueCnt. > 0)) %then %do;
            %put ERROR: No Group-by variables were specified inside dataset &ds_in_aggregation_config.. Skipping execution..;
            %return;
         %end;

         %if(%eval(&TotGroupByCnt. + &TotAggrVarCnt. + &TotUniqueCnt.) > 0) %then %do;
            %let aggregate_flg = Y;

            /* Check if we need to perform conditional aggregation */
            %if %sysevalf(%superq(conditional_aggregation_clause) ne, boolean) %then
               %let conditional_aggregation_flg = Y;
         %end;

      %end;
      %else %do;
         %put ERROR: input dataset &ds_in_aggregation_config. does not exist. Skipping execution..;
         %return;
      %end;
   %end;


   /* *************************************************** */
   /*       Create Data Extract (View or Table)           */
   /* *************************************************** */

   /* Process the where clause */
   %if(%sysevalf(%superq(where_clause) ne, boolean)) %then
      %let where_clause = %bquote((where = ( %sysfunc(prxchange(s/[""]/""/i, -1, %superq(where_clause))) )));

   /* Create temporary table needed to derive the output column structure */
   data __tmp_data_structure;
      set
         %if(%sysevalf(%superq(out_ds_structure) ne, boolean)) %then %do;
            &out_ds_structure.  (obs = 0)
         %end;

         %do i = 1 %to &TotTables.;
            _dslib.&&source_ds_&i.. (obs = 0)
         %end;
      ;

      stop;
   run;

   /* Get detail data */
   data &out_ds.
         %if(%sysevalf(%superq(drop_list) ne, boolean)) %then
            (drop=&drop_list.);
         %if(%upcase(&out_type.) in VIEW DEFERRED) %then %do;
            / view = &out_ds. (source=nosave)
               %if(%sysevalf(%superq(pwd) ne, boolean)) %then
                  (alter = &pwd.);

         %end;
         ;

      drop __rownum__;
      retain __rownum__ 0;

      /* Assign all columns */
      attrib
         /* Get variables definition */
         %rsk_get_attrib_def(ds_in = __tmp_data_structure)
      ;

      /* Loop through all partition libraries */
      length
         __rc__ 8.
         __msg__ $512.
         __last__ 8.
      ;
      drop
         __rc__
         __msg__
         __last__
      ;
      retain __last__ 0;

      /* Initialize Aggregation variables */
      %if(&aggregate_flg. = Y) %then %do;
         /* Declare aggregation variables */
         attrib
            __hash_key__  length = $64.
            __row_count__ length = 8.

            /* Weight summary variables  */
            %do i = 1 %to &TotWeightCnt.;
               %if(%rsk_varexist(__tmp_data_structure, &&aggr_weight_var_name_&i..)) %then %do;
                  %rsk_get_attrib_def(ds_in = __tmp_data_structure
                                      , keep_vars = &&aggr_weight_var_name_&i..
                                      , rename_vars = &&aggr_weight_var_name_&i.. = &&aggr_weight_summary_var_&i..
                                      )
               %end;
               %else %do;
                  &&aggr_weight_summary_var_&i.. length = 8.
               %end;
            %end;
            /* Aggregation summary variables */
            %do i = 1 %to &TotAggrVarCnt.;
               /* Set the length of the aggregation variable to match the definition of the original variable to be aggregated */
               %if(%rsk_varexist(__tmp_data_structure, &&aggr_var_name_&i..)) %then %do;
                  %rsk_get_attrib_def(ds_in = __tmp_data_structure
                                      , keep_vars = &&aggr_var_name_&i.
                                      , rename_vars = &&aggr_var_name_&i. = &&aggr_summary_var_&i..
                                      )

                  %if(not %rsk_varexist(__tmp_data_structure, &&aggr_summary_alias_&i..)) %then %do;
                     %if(%sysevalf(%superq(aggr_final_expression_&i.) = __row_count__, boolean)) %then %do;
                        /* This summary variable is a count. Just set the length */
                        &&aggr_summary_alias_&i.. length = 8.
                     %end;
                     %else %do;
                        %rsk_get_attrib_def(ds_in = __tmp_data_structure
                                            , keep_vars = &&aggr_var_name_&i.
                                            , rename_vars = &&aggr_var_name_&i. = &&aggr_summary_alias_&i..
                                            )
                     %end;
                  %end;
               %end;
               %else %do;
                  &&aggr_summary_var_&i.. length = 8.
               %end;
            %end;
            /* Unique summary variables */
            %do i = 1 %to &TotUniqueCnt.;
               &&aggr_unique_summary_var_&i.. length = 8.
            %end;
         ;

         drop
            __rcAggr__
            __hash_key__
            __row_count__
            %do i = 1 %to &TotWeightCnt.;
               &&aggr_weight_summary_var_&i..
            %end;
            %do i = 1 %to &TotAggrVarCnt.;
               &&aggr_summary_var_&i..
            %end;
         ;

         /* Summary Hash table for storing aggregated results */
         declare hash hSummary();
         declare hiter hSummaryIter("hSummary");
         hSummary.defineKey("__hash_key__");
         hSummary.defineData("__row_count__"
                              %do i = 1 %to &TotGroupByCnt.;
                                 , "&&aggr_groupBy_var_&i.."
                              %end;
                              %do i = 1 %to &TotWeightCnt.;
                                 , "&&aggr_weight_summary_var_&i.."
                              %end;
                              %do i = 1 %to &TotAggrVarCnt.;
                                 , "&&aggr_summary_var_&i.."
                              %end;
                              %do i = 1 %to &TotUniqueCnt.;
                                 , "&&aggr_unique_summary_var_&i.."
                              %end;
                             );
         hSummary.defineDone();

         %do i = 1 %to &TotUniqueCnt.;
            /* Hash table for keeping track of unique values of a variable */
            declare hash hUnique&i.();
            hUnique&i..defineKey("__hash_key__", "&&aggr_unique_var_&i..");
            hUnique&i..defineDone();
         %end;

      %end; /* %if(&aggregate_flg. = Y) */


      %if(&TotTables. > 0) %then %do;

         /* Assign the libname */
         __rc__ = libname ("_srclib", ,"&lib_engine.", "%sysfunc(prxchange(s/[""]/""/i, -1, %superq(source_lib_connection_options)))");

         /* Check for errors or expiration */
         if __rc__ ne 0 or (today() - %sysfunc(today())) > &dte. then do;
            if __rc__ ne 0 then
               /* There was an issue while assigning the libname */
               __msg__ = sysmsg();
            else
               /* The view has expired */
               __msg__ = "ERROR: Access to the data has expired (Creation Date: %sysfunc(today(), yymmddp10.), Validity: &dte. days).";

            /* Write error message to the log and exit */
            put __msg__;

            /* Deassign the libname */
            __rc__ = libname ("_srclib");
            stop;
         end;

         array __dsid_list__ {&TotTables.} _temporary_;
         /* Loop through all partitions */
         %do i = 1 %to &TotTables.;
             %if %rsk_dsexist(_dslib.&&source_ds_&i..) %then %do ;
                /* Open the current partition table and get handle id */
                __dsid_list__[&i.] = open("_srclib.&&source_ds_&i.. %unquote(&where_clause.)");
                /* Check for errors */
                if(__dsid_list__[&i.] = 0) then do;
                   /* Get the error message and print it to the log */
                   __msg__= sysmsg();
                   put __msg__;
                end;
                else
                   /* Link variables to the DDV */
                   call set(__dsid_list__[&i.]);
             %end ;
             %else %do;
                 %put WARNING: Table &&source_ds_&i.. does not exist.;
             %end ;
         %end;

         /* Loop through all datasets */
         drop __i__;
         do __i__ = 1 to dim(__dsid_list__);
            /* Process the current partition (if we were able to open it) */
            if(__dsid_list__[__i__]) then do;
               /* Fetch first observation */
               __rc__ = fetch(__dsid_list__[__i__]);
               do while(__rc__ = 0);

      %end;

                  /* Increment row number */
                  __rownum__ + 1;

                  /* Include custom code */
                  %unquote(&custom_code.);

      %if(&TotTables. > 0) %then %do;


                     /* Perform aggregation if required */
                     %if(&aggregate_flg. = Y) %then %do;

                        /* Initialize all aggregation variables */
                        call missing(__row_count__
                                       %do i = 1 %to &TotWeightCnt.;
                                          , &&aggr_weight_summary_var_&i..
                                       %end;
                                       %do i = 1 %to &TotAggrVarCnt.;
                                          , &&aggr_summary_var_&i..
                                          %if(%upcase(&&aggr_summary_alias_&i..) ne %upcase(&&aggr_var_name_&i.)) %then
                                          , &&aggr_summary_alias_&i..;
                                       %end;
                                       %do i = 1 %to &TotUniqueCnt.;
                                          , &&aggr_unique_summary_var_&i..
                                       %end;
                                    );

                        %if(&conditional_aggregation_flg. = Y) %then %do;
                           /* Update the aggregation hash table only if we match the specified condition */
                           if(%unquote(&conditional_aggregation_clause.)) then do;
                        %end;

                        /* Compute Hash Key for each group-by combination */
                        __hash_key__ = hashing("sha256"
                                                , catx("|"
                                                         %do i = 1 %to &TotGroupByCnt.;
                                                            , &&aggr_groupBy_var_&i..
                                                         %end;
                                                         )
                                                );

                        /* Lookup the aggregation variables for the given group-by combination */
                        __rcAggr__ = hSummary.find();

                        /* Increment the row counter */
                        __row_count__ + 1;
                        /* Aggregate the weights */
                        %do i = 1 %to &TotWeightCnt.;
                           &&aggr_weight_summary_var_&i.. = SUM(&&aggr_weight_summary_var_&i.., &&aggr_weight_var_name_&i..);
                        %end;
                        /* Aggregate the variables */
                        %do i = 1 %to &TotAggrVarCnt.;
                           %if %sysevalf(%superq(aggr_func_&i.) ne, boolean) %then %do;
                              &&aggr_summary_var_&i.. = &&aggr_func_&i..(&&aggr_summary_var_&i.., &&aggr_expression_&i..);
                           %end;
                        %end;
                        /* Count distinct values */
                        %do i = 1 %to &TotUniqueCnt.;
                           /* Check if the current value of the variable has been already processed */
                           if hUnique&i..check() ne 0 then do;
                              /* This is a new value: increase the counter and add the entry to the hash table */
                              &&aggr_unique_summary_var_&i.. + 1;
                              hUnique&i..add();
                           end;
                        %end;

                        /* Update summary stats */
                        hSummary.replace();

                        %if(&conditional_aggregation_flg. = Y) %then %do;
                           end; /* if(%unquote(&conditional_aggregation_clause.)) then do; */
                           else do;
                              /* Output the detailed record */
                              output &out_ds.;
                           end;
                        %end;

                     %end; /* %if(&aggregate_flg. = Y) */
                     %else %do;
                        output &out_ds.;
                     %end;

                  /* Fetch next observation */
                  __rc__ = fetch(__dsid_list__[__i__]);
               end; /* while(__rc__ = 0) */

               /* Close dataset */
               __rc__ = close(__dsid_list__[__i__]);
            end; /* if(__dsid_list__[__i__]) */

            /* Mark the last record */
            if(__i__ = dim(__dsid_list__)) then do;
               __last__ = 1;
               /* Deassign the libname */
               __rc__ = libname ("_srclib");
            end;

            /* Finalize aggregation */
            %if(&aggregate_flg. = Y) %then %do;
               if __last__ then do;
                  /* Reset all variables */
                  call missing(of %rsk_getvarlist(__tmp_data_structure));
                  %if(&keep_all_flg. = Y) %then %do;
                     /* Assign aggregation symbol to all the character variables */
                     array __all_chars__{*} _character_;
                     drop __j__;
                     do __j__ = 1 to dim(__all_chars__);
                        __all_chars__[__j__] = "&aggregation_symbol.";
                     end;
                  %end;
                  %else %do;
                     /* Keep only the variables that were involved in the aggregation */
                     keep
                        %do i = 1 %to &TotGroupByCnt.;
                           &&aggr_groupBy_var_&i..
                        %end;
                        %do i = 1 %to &TotAggrVarCnt.;
                           &&aggr_summary_alias_&i..
                        %end;
                        %do i = 1 %to &TotUniqueCnt.;
                           &&aggr_unique_summary_var_&i..
                        %end;
                     ;
                  %end;
                  /* Iterate over the Summary hash to retrieve all the group-by and aggregated variables */
                  do while(hSummaryIter.next() = 0);

                     /* Finalize the aggregation */
                     %do i = 1 %to &TotAggrVarCnt.;
                        &&aggr_summary_alias_&i.. = &&aggr_final_expression_&i..;
                     %end;

                     /* Include post aggregation code */
                     %unquote(&post_aggregation_code.);

                     /* Output aggregated record */
                     output &out_ds.;
                  end; /* do while(hSummaryIter.next() = 0) */

                  /* Clear the Hash tables to release memory */
                  hSummary.clear();
                  %do i = 1 %to &TotUniqueCnt.;
                     hUnique&i..clear();
                  %end;

               end; /* if __last__ then do; */
            %end; /* %if(&aggregate_flg. = Y) %then %do; */

         end; /* do __i__ = 1 to dim(__dsid_list__) */

      %end; /* %if(&TotTables. > 0) */
      %else %do;
         %if(&aggregate_flg. = Y and &keep_all_flg. = N) %then %do;
            /* Keep only the variables that were involved in the aggregation */
            keep
               %do i = 1 %to &TotGroupByCnt.;
                  &&aggr_groupBy_var_&i..
               %end;
               %do i = 1 %to &TotAggrVarCnt.;
                  &&aggr_summary_alias_&i..
               %end;
               %do i = 1 %to &TotUniqueCnt.;
                  &&aggr_unique_summary_var_&i..
               %end;
            ;
         %end;
         /* There are no tables to process. */
         stop;
      %end;

      /* Deassign the libname in case any error has occurred */
      if _error_ then
         __rc__ = libname ("_srclib");
   run;

%mend;