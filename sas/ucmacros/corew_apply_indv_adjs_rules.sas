/*
 Copyright (C) 2022-2024 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor corew_apply_indv_adjs_rules.sas

   \brief   Apply individual adjustment rules to detail level

   \param [in] ds_in Input dataset containing detail level data.
   \param [in] ds_in_adj Input dataset containing adjustments to apply to detail level data
   \param [in] epsilon Minimum threshold for delta values.  Any adjustments with a delta value smaller than epslion will not be stored in the output dataset
   \param [in] merge_by_vars List of space separated variables to use to merge &ds_in and &ds_in_adj
   \param [in] ds_in_var_dependency Input dataset containing the variable dependencies for the adjustment variables
   \param [in] exclude_vars_list (Optional) List of columns to exclude from adjustment dataset (&ds_in_adj).
      Should not be considered as measure variables for adjustment (example: REPORTING_DT HORIZON FORECAST_TIME PROCESSED_DTTM).
   \param [out] ds_out_delta Name of the output dataset containing the details of each record that was modified.

   \details
   This macro applies a set of individual allocation rules to the input table &DS_IN. based on the adjustments defined inside the table &ds_in_adj.

   Any numeric variable in &ds_in_adj that has a matching column name in ds_in (excluding reporting_dt and any variables specified in &exclude_vars_list) will be considered a measure variable for adjustments.

   The section below describe how each rule is applied based on the adjustment type inside the adjustment table. <br>

   The following notation is used:
   - \f$Adj\f$ is the adjustment value that must be allocated to details
   - \f$v_n\f$ is each of \f$N\f$ detail values of the <i>&lt;Measure&gt;</i> column in the input table matching the rule's filtering condition (\f$n = 1 to N\f$)
   - \f$v'_n\f$ is the allocated value of the <i>&lt;Measure&gt;</i> column in the output table

   The table below shows how the allocation rules are applied based on the configuration selection

   | Allocation Method | Absolute Adjustment      | Relative Adjustment                  | Increment Adjustment         |
   |-------------------|--------------------------|--------------------------------------|------------------------------|
   | INDIVIDUAL        | \f[v'_n = Adj\f]         | \f[v'_n =  v_n \cdot (1 + Adj)\f]    | \f[v'_n = (v_n + Adj)\f]     |


   \ingroup macroUtils

   \author  SAS Institute Inc.
   \date    2017
*/
%macro corew_apply_indv_adjs_rules(ds_in =
                                 , ds_in_adj =
                                 , epsilon = 1e-10
                                 , merge_by_vars =
                                 , ds_in_var_dependency =
                                 , exclude_vars_list =
                                 , ds_out_delta = modified_delta
                                 , ds_out_exceptions = ia_exceptions
                                 , outModelCasLib =
                                 , casSessionName = casauto
                                 );
   %local
      current_dttm
      ds_in_adj_num_colnames
      ds_in_num_colnames
      ds_in_adj_num_colnames_subset
      exclude_prx
      adj_colnames
      drop_colnames
      var_dependency_flg
      dependent_varnames
      var_check_list
      quoted_adj_colnames
      quoted_merge_by_vars_1
      quoted_merge_by_vars_2
      dt_fmts
      dttm_fmts
      tm_fmts
      dt_tm_dttm_vars
      i
      outCasLibref
      maxHorizon
   ;

   %let outCasLibref = %rsk_get_unique_ref(type = lib, engine = cas, args = caslib="&outModelCaslib." sessref=&casSessionName.);

   /* Get only the max horizon in the results */
   proc fedsql sessref=&casSessionName.;
      create table "&outModelCasLib.".max_horizon {options replace=true} as
      select max(horizon) as max_horizon
      from "&outModelCasLib.".&ds_in.
            ;
   quit;

   data _null_;
      set &outCasLibref..max_horizon;
      call symputx("maxHorizon", max_horizon, "L");
   run;

   %let current_dttm = "%sysfunc(datetime(), datetime21.)"dt;

   /* Get all numeric columns from ds_in_adj - dataset with data adjustments */
   %let ds_in_adj_num_colnames = %rsk_getvarlist(&outCasLibref..&ds_in_adj., type=N);

   /* Get all numeric columns from ds_in - dataset to be adjusted */
   %let ds_in_num_colnames = %rsk_getvarlist(&outCasLibref..&ds_in., type=N);

   /*************************************************************************************************************************************/
   /* 1 - Get list of 'numeric' columns that are coming from adjust dataset(ds_in_adj) and exist also in dataset(ds_in) to be adjusted) */
   /*************************************************************************************************************************************/

   %if(%sysevalf(%superq(exclude_vars_list) eq, boolean)) %then %do;
      %let exclude_vars_list=REPORTING_DT;
   %end;
   /* start by exclude the variables from the list using the macro parameter: 'exclude_vars_list' */
   %let exclude_prx=\b%sysfunc(prxchange(s/ +/ ?\b|\b/oi,-1,&exclude_vars_list.))\b;
   %let ds_in_adj_num_colnames_subset = %sysfunc(prxchange(s/&exclude_prx//oi,-1,&ds_in_adj_num_colnames));
   /* Reformulate the list - new macro varaible to be used in step 2 below */
   %let adj_colnames=;
   %do i = 1 %to %sysfunc(countw(&ds_in_adj_num_colnames_subset., %str( )));
      %let curr_var = %scan(&ds_in_adj_num_colnames_subset., &i., %str( ));
      %if %sysfunc(prxmatch(/\b&curr_var.\b/i, &ds_in_num_colnames.)) %then
         %let adj_colnames = &adj_colnames. &curr_var.;
   %end;

   /* Convert adjustment column names to quoted comma-separated list */
   %let quoted_adj_colnames = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &adj_colnames.));
   %let quoted_adj_colnames = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&quoted_adj_colnames.)));

   /* Process the var dependency table if it was provided */
   %let var_dependency_flg = N;

   %if(%sysevalf(%superq(ds_in_var_dependency) ne, boolean)) %then %do;
      %let var_dependency_flg = Y;
      proc sql;
         create table __tmp_var_dependency__ as
         select
            upcase(trigger_var_name) as __trigger_var_name__
            , order_no as __order_no__
            , dependent_var_name as __dependent_var_name__
            , expression_txt as __expression_txt__
         from
            &ds_in_var_dependency.
         order by
            trigger_var_name
            , order_no
         ;
      quit;

      /* Get list of dependent variables from var dependency table */

      proc sql;
         select distinct __dependent_var_name__
            into :dependent_varnames separated by ' '
         from __tmp_var_dependency__
         where __trigger_var_name__ in (&quoted_adj_colnames.);
      quit;

   %end;

   /*************************************************************************************************************************************************************************/
   /* 2 - Get list of 'numeric' non-adjustment columns to drop from dataset(ds_in) (excluding: columns from both datasets | date/datetime/time columns | dependent columns) */
   /*************************************************************************************************************************************************************************/

   /* Get list of date/time/datetime variables from input table (should be preserved in movement records) */
   %let dt_fmts = %rsk_get_dtm_formats(type = date);
   %let dttm_fmts = %rsk_get_dtm_formats(type = datetime);
   %let tm_fmts = %rsk_get_dtm_formats(type = time);
   %let dt_tm_dttm_vars = %rsk_getvarlist(&outCasLibref..&ds_in., format = &dt_fmts.|&dttm_fmts.|&tm_fmts.);

   /* start by exclude the variables from the list 'adj_colnames' and 'dependent_varnames' from step 1, 'dt_tm_dttm_vars' */
   %let exclude_prx=\b%sysfunc(prxchange(s/ +/ ?\b|\b/oi,-1,&exclude_vars_list. &adj_colnames. &dependent_varnames. &dt_tm_dttm_vars.))\b;
   %let drop_colnames = %sysfunc(prxchange(s/&exclude_prx.//oi,-1,&ds_in_num_colnames.));

   /* Get list of quoted comma-separated merge by columns */
   %let quoted_merge_by_vars_1 = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &merge_by_vars.));
   %let quoted_merge_by_vars_2 = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&quoted_merge_by_vars_1.)));

   /* Make sure there are adjustment columns */
   %if %sysevalf(%superq(adj_colnames) =, boolean) %then %do;
      %put ERROR: There are no valid adjustment columns found in &outCasLibref..&ds_in_adj.;
      %return;
   %end;

   /* Make sure there are no adjustment variables dependent on other adjustment variables in &outCasLibref..&ds_in_adj */
   %if(&var_dependency_flg. = Y) %then %do;
      data _null_;
         set __tmp_var_dependency__;
         where prxmatch(cats("/\b",__trigger_var_name__,"\b/i"), "&adj_colnames.") > 0;
         if prxmatch(cats("/\b",__dependent_var_name__,"\b/i"), "&adj_colnames.") > 0 then do;
            put "ERROR: Adjustment variable " __dependent_var_name__ " is dependent on adjustment variable " __trigger_var_name__;
            stop;
         end;
      run;
   %end;

   /* Count the number of adjustment variables */
   %let tot_adj_vars = %sysfunc(countw(&adj_colnames., %str( )));

   /* Build adjustment conditions */
   %let tot_rules = 0;
   data _null_;

      set &outCasLibref..&ds_in_adj. end = last;

      %if(&var_dependency_flg. = Y) %then %do;
         if _N_ = 0 then
            set __tmp_var_dependency__;

         /* Setup lookup for retrieving the dependency rules */
         if _N_ = 1 then do;
            declare hash hDep(dataset: "__tmp_var_dependency__", multidata: "yes");
            hDep.defineKey("__trigger_var_name__");
            hDep.defineData("__dependent_var_name__", "__expression_txt__");
            hDep.defineDone();
         end;
      %end;

      /* Store the adjustment information for each adjustment variable */
      %do j = 1 %to &tot_adj_vars.;
         %let measure_col_&j. = %upcase(%scan(&adj_colnames., &j., %str( )));

         %if(&var_dependency_flg. = Y) %then %do;
            /* Retreive dependency rules for each adjustment variable */
            __trigger_var_name__ = "&&measure_col_&j..";
            __k__ = 0;
            call missing(__dependent_var_name__, __expression_txt__);
            do while(hDep.do_over() = 0);
               __k__ = __k__ + 1;
               call symputx(cats("dependent_var_&j.", "_", __k__), __dependent_var_name__, "L");
               call symputx(cats("expression_txt_&j.", "_", __k__), __expression_txt__, "L");
               call missing(__dependent_var_name__, __expression_txt__);
            end;
            call symputx(cats("dependent_var_cnt_", &j.), __k__, "L");
         %end;
      %end;

   run;

   /* Make sure we have any adjustments to process */
   %if(%rsk_attrn(&outCasLibref..&ds_in_adj., nobs) eq 0) %then %do;
      %put ERROR: Input dataset &outCasLibref..&ds_in_adjustments. is empty. Skipping execution..;
      %return;
   %end;

   /* Generate drop option for system-generated columns in ds_in_adj if they exist */
   %let ds_in_adj_drop =;
   %let var_check_list=table_id project_id load_id workgroup;
   %do i = 1 %to %sysfunc(countw(&var_check_list., %str( )));
      %let curr_var = %scan(&var_check_list., &i., %str( ));
      %if %rsk_varexist(&outCasLibref..&ds_in_adj., &curr_var.) %then
         %let ds_in_adj_drop = &ds_in_adj_drop. &curr_var.;
   %end;
   %if %sysevalf(%superq(ds_in_adj_drop) ne, boolean) %then
      %let ds_in_adj_drop = drop = &ds_in_adj_drop.;

   /* Merge adjustment dataset and input dataset */
   data &outCasLibref..ds_in_merge
        &outCasLibref..exceptions_list (keep=&merge_by_vars.);
      merge &outCasLibref..&ds_in. (where=(horizon = &maxHorizon.) drop=
                        movement_id
                        movement_type
                        movement_type_cd
                        movement_category
                        movement_desc
                   in = in_ds_in
                  )
            &outCasLibref..&ds_in_adj. ( &ds_in_adj_drop.
                        rename=(
                              %do j = 1 %to &tot_adj_vars.;
                                 &&measure_col_&j.. = adj_col_&j.
                              %end;
                              )
                        in = in_adj
                      )
      ;
      by &merge_by_vars.;
      /* Only store movements for records that exist in ds_in and ds_in_adj */
      if in_adj and in_ds_in then output &outCasLibref..ds_in_merge;
      else if in_adj and not in_ds_in then output &outCasLibref..exceptions_list;
   run;

   /* Enrich the exceptions list with adjustment data */
   data &outCasLibref..&ds_out_exceptions.;
      set &outCasLibref..exceptions_list;
      if _N_ = 0 then
         set &outCasLibref..&ds_in_adj.;
      if _N_=1 then do;
         declare hash hAdj(dataset: "&outCasLibref..&ds_in_adj.", multidata: "yes");
         hAdj.defineKey(&quoted_merge_by_vars_2.);
         hAdj.defineData(all: "yes");
         hAdj.defineDone();
      end;
      _rc_ = hAdj.find();
      drop _rc_;
   run;

   /* Calculate the delta value for each adjustment */
   data &outCasLibref..&ds_out_delta.;
      set &outCasLibref..ds_in_merge end = last;

      /* Initialize nonzero delta field to check if any adjustments have a delta > epsilon */
      nonzero_delta = "N";

      /* For each adjustment variable, get its delta and set it to its IA value */
      /*Note: Don't set the adjustment variable to its delta until all variable dependencies are run*/
      %do j = 1 %to &tot_adj_vars.;

         /* Apply individual adjustment */
         if adjustment_type = "RELATIVE" then do;
            /* Relative adjustment */
            __delta_value__&j. = &&measure_col_&j.. * adj_col_&j.;
            &&measure_col_&j.. = &&measure_col_&j.. * (1 + adj_col_&j.);
         end;
         else if adjustment_type = "INCREMENT" then do;
            /* Incremenetal adjustment */
            __delta_value__&j. = adj_col_&j.;
            &&measure_col_&j.. = sum(&&measure_col_&j.., adj_col_&j.);
         end;
         else do;
            /* Absolute adjustment */
            __delta_value__&j. = sum(adj_col_&j., -&&measure_col_&j..);
            &&measure_col_&j.. = adj_col_&j.;
         end;

         /* At least one adjustment variable has a delta > epsilon */
         if abs(__delta_value__&j.) > &epsilon then nonzero_delta = "Y";

      %end;

      /*run all variable dependencies*/
      %if(&var_dependency_flg. = Y) %then %do;

         /* For each adjustment variable, get the delta of each of its dependent vars */
         /* Note: Don't set the dependent variable to their deltas until all variable dependencies are run*/
         %do j = 1 %to &tot_adj_vars.;

         %do k = 1 %to &&dependent_var_cnt_&j..;
            __orig_value__&j._&k.= &&dependent_var_&j._&k..; /*store dependent variable's original value*/
         %end;

         /* Process dependencies */
         %do k = 1 %to &&dependent_var_cnt_&j..;
            /* Compute the delta for the dependent variable */
            __delta_value__&j._&k. = sum(&&expression_txt_&j._&k.., - &&dependent_var_&j._&k..);
            &&dependent_var_&j._&k.. = &&expression_txt_&j._&k..; /*update dependent variable to new value for subsequent dependencies in this trigger var*/
         %end;

         %do k = 1 %to &&dependent_var_cnt_&j..;
            &&dependent_var_&j._&k..=__orig_value__&j._&k.; /*set dependent variable back to original value for dependencies in subsequent trigger vars*/
            drop __orig_value__&j._&k.;
         %end;

         %end;

         /*set each dependent variable to its delta value now that dependencies are done*/
         %do j = 1 %to &tot_adj_vars.;

         %do k=1 %to &&dependent_var_cnt_&j..;
               &&dependent_var_&j._&k.. = __delta_value__&j._&k.;
         %end;

         %end;

      %end; /* %if(&var_dependency_flg. = Y) */

      /*set each adjustment variable to its delta value now that dependencies are done*/
      %do j = 1 %to &tot_adj_vars.;
         &&measure_col_&j.. = __delta_value__&j.;
      %end;

      /* Output record to the delta table if delta value > epsilon */
      if nonzero_delta = "Y" then output &outCasLibref..&ds_out_delta.;

      if last then do;
         drop
            adjustment_type
            &drop_colnames.
            adj_col_:
            __delta_value__:
            nonzero_delta
         ;
      end;
   run;

   libname &outCasLibref. clear;

%mend;