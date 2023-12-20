/*
 Copyright (C) 2022 SAS Institute Inc. Cary, NC, USA
*/

/**
   \file
\anchor corew_synth_sample_elimination
   \brief   Randomly sample (without replacement) from an input sampling frame to meet negative projections

   \param [in] inSamplingFrame input table that we will draw samples (eliminate) from.
      Note: This can be a SAS or CAS table.  If it is a CAS table, it must be given as a valid 2-level SAS table (<libref>.<table_name>)
   \param [in] ds_in_projection input SAS dataset that contains projections by segments in the sampling frame for target variables
      over 1 or more horizons (and optionally for different scenarios).
   \param [in] ds_in_allocation (optional) input SAS dataset that contains allocation percentages for segments WITHIN the ds_in_projection segments.
      Note: Allocation percentages can be given for all scenarios or for individual scenarios for a given segment:
         All scenarios: Set the scenario column value to empty (missing value)
         Individual scenarios: Set the scenario column value to the matching scenario in ds_in_projection
         Note: For a given segment, allocation percentages must either be for all scenarios (scenario missing) or for individual scenarios.
         A mix of both cases is not supported.
   \param [in] target_var Target variable name.  Sampling is done only for these rows in ds_in_projection.  Samples are drawn until the projection
      amounts for this variable are hit.
   \param [in] relativeValue_var: name of the relative value variable in ds_in_projection.  This is the projection amount of the target variable
      that needs reached for each horizion.  (Default: relativeValue)
   \param [in] alloc_by_vars Space-separated list of allocation variables in the ds_in_allocation that contain allocation percentages within each segment.
   \param [in] percentage_var Name of the allocation percentage variable in ds_in_allocation (Default: allocationWeight)
   \param [in] horizon_var Name of the horizon variable in ds_in_projection (Default: activationHorizon)
   \param [in] scenario_var Name of the scenario variable in ds_in_projection and ds_in_allocation (Default: scenario)
   \param [in] segmentFilterVar Name of the variable in ds_in_projection and ds_in_allocation that contains a SAS where-clause filter string to identify
      a specific segment.  The filter values for this variable should use single quotes (not double quotes). (Default: segmentFilterSas)
         Ex: segmentFilterSas="INSTTYPE='HELOCs'"
   \param [in] id_var The name of the key variable identifying each row (Default: instid)
   \param [in] id_var_length (optional) The length of the key identifier variable.  If not provided, warnings may be thrown during elimination that the length of the id variable in CAS data step merges do not match.
   \param [in] custom_seed Seed used in PROC SURVEYSELECT.  You will get the same samples draw for a given custom seed every time.
   \param [in] reproducible_flag If Y, if inSamplingFrame is a CAS table, move each segment into SAS before sampling.  This is needed since
      PROC SURVEYSELECT can only guarantee reproducibility (for a given custom_seed) if the sampling frame is a SAS table (not CAS).  (Y/N) (Default: N)
      Note that this can add runtime, since inSamplingFrame must be moved into SAS in this case.
   \param [in] debug Whether or not to output extra debug info to the log (true/false) (Default: false)
   \param [in] simulation_factor A multiplier to the estimated number of samples drawn for each projection.  The estimated number of samples needed
      to match a projection for a given segment, scenario, horizon, and allocation variable within that segment is:
            _NSIZE_ = simulation_factor.*abs(relativeValue_var)/<targetVarMean_segment>)*percentage_var;
      If simulation_factor is higher, _NSIZE_ is higher.  This means more samples will be drawn so we'll be more likely to draw enough
      rows to meet the projection.  However, it could also mean additional runtime. (Default 1.5)
   \param [in] max_sampling_reps The maximum number of times to try and meet remaining projections that have not been reached
      for a given segment, scenario, horizon, and allocation variable.  (We sample, select observations from the sample
      until the target projection is reached, and if it isn't repeat a max of max_sampling_reps times.) (Default: 5)
   \param [in] projection_epsilon A percentage under a projection that is considered sufficient (Default: .01)
      Ex: projection_epsilon=.01 --> drawing (1 - .01)*projection is considered meeting the projection.
   \param [out] outSample Name of the output SAS table containing the selected samples to meet projections.

   \details
   This macro randomly draws rows from an input sampling frame (inSamplingFrame) using sampling without replacement until the projections in an input dataset (ds_in_projection) are met.  The projections are given by segment (segmentFiterSas),scenario, and horizon.

   The projections within each segment and scenario can be allocated by percentages for additional variables given in an input allocation
   dataset (ds_in_allocation).

   <b>Example:</b>

   1) Set up the environment (set SASAUTOS and required LUA libraries).
   \code
      %let core_root_path=/riskcirruscore/core/code_libraries/release-core-%SYSGET(SAS_RISK_CIRRUS_CADENCE);
      option insert = (SASAUTOS = ("&core_root_path./spre/sas/ucmacros"));
      filename LUAPATH ("&core_root_path./spre/lua");
   \endcode

   2) Run the macro to generate enough samples to meet projections
   \code
      %corew_synth_sample_elimination(inSamplingFrame = credit_portfolio
                                 , ds_in_projection = _tmp_analysis_proj_plus_
                                 , outSample = eliminated_positions
                                 , target_var = unpaid_balance_amt
                                 , custom_seed = 12345
                                 , debug = true
                                 );

    \endcode

   \author  SAS Institute Inc.
   \date    2023
*/

%macro corew_synth_sample_elimination(inSamplingFrame =
                                    , ds_in_projection =
                                    , ds_in_allocation =
                                    , outSample = eliminated_positions
                                    , target_var =
                                    , alloc_by_vars =
                                    , relativeValue_var = relativeValue
                                    , percentage_var = allocationWeight
                                    , horizon_var = activationHorizon
                                    , scenario_var = scenario
                                    , segmentFilterVar = segmentFilterSas
                                    , id_var = instid
                                    , id_var_length =
                                    , custom_seed =
                                    , reproducible_flag = N
                                    , debug = false
                                    , simulation_factor = 1.5
                                    , max_sampling_reps = 5
                                    , projection_epsilon = .01
                                    );

   %local
      samp_frame_libref samp_frame_engine sampsize_table
      allocation_flag alloc_vars_flag
      seg b s rep next_rep
      targets_reached
      unquoted_csv_allocvar_list quoted_csv_allocvar_list sql_a_csv_allocvar_list sql_b_csv_allocvar_list sql_join_csv_allocvar_list
      ;

   %if (%sysevalf(%superq(inSamplingFrame) eq, boolean)) %then %do;
      %put ERROR: inSamplingFrame is required.;
      %return;
   %end;

   %if (%sysevalf(%superq(ds_in_projection) eq, boolean)) %then %do;
      %put ERROR: ds_in_projection is required.;
      %return;
   %end;

   %if (%sysevalf(%superq(target_var) eq, boolean)) %then %do;
      %put ERROR: target_var is required.;
      %return;
   %end;

   /* If reproducible_flag=Y, the sampling frame must be pulled into SAS and sampled from SAS, so always
   use the WORK libref in that case.  This is because SURVEYSELECT can only reproduce results in SAS sampling frames. */
   %let samp_frame_libref = WORK;
   %if %upcase("&reproducible_flag.") ne "Y" %then %do;
      %if %sysfunc(find(&inSamplingFrame., %str(.))) %then
         %let samp_frame_libref = %scan(&inSamplingFrame., 1, %str(.));
   %end;
   %let samp_frame_engine = %rsk_get_lib_engine(&samp_frame_libref.);

   /* Delete the _tmp_out_sample_selected_ dataset if it already exists */
   %if (%rsk_dsexist(&outSample.)) %then %do;
      proc sql;
         drop table &outSample.;
      quit;
   %end;

   /* If the allocation percentages table wasn't provided at all, or if it has no observations,
   create a dummy allocation table for easier processing */
   %let allocation_flag=Y;
   %if (%sysevalf(%superq(ds_in_allocation) eq, boolean)) %then
      %let allocation_flag=N;
   %else %if %rsk_getattr(&ds_in_allocation., NOBS) eq 0 %then
      %let allocation_flag=N;

   %if &allocation_flag.=N %then %do;
      %let ds_in_allocation=__tmp_alloc_ds__;
      data &ds_in_allocation.;
         length &segmentFilterVar. $32000 &scenario_var. $200;
         stop;
      run;
   %end;

   /* Read the projection table into a macrovariable array by segment */
   proc sort data=&ds_in_projection. out=_tmp_proj_srt_; by &segmentFilterVar. &scenario_var. &horizon_var.; run;
   data _null_;
      set _tmp_proj_srt_ end=last;
      length proj_scenarios_list $10000;
      retain segmentNum scenarioNum 0 proj_scenarios_list "";
      by &segmentFilterVar. &scenario_var.;
      if first.&segmentFilterVar. then do;
         segmentNum = segmentNum +1;
         scenarioNum = 0;
         proj_scenarios_list="";
         call symputx(catx("_", "segment_filter", put(segmentNum, 8.)), &segmentFilterVar., "L");
      end;
      if first.&scenario_var. then do;
         scenarioNum = scenarioNum+1;
         call symputx(catx("_", "segment_scenario", put(segmentNum, 8.), put(scenarioNum, 8.)), &scenario_var., "L");
         proj_scenarios_list = catt(proj_scenarios_list, " '", &scenario_var., "'");
      end;

      if last.&segmentFilterVar. then do;
         call symputx(catx("_", "num_scenarios", put(segmentNum, 8.)), scenarioNum, "L");
         call symputx(catx("_", "proj_scenarios_list", put(segmentNum, 8.)), proj_scenarios_list, "L");
      end;
      if last then do;
         call symputx("num_segments", segmentNum, "L");
      end;
   run;

   /* Segment Loop: draw samples to meet projections for all scenarios and horizons, 1 segment at a time */
   %do seg=1 %to &num_segments.;

      %do scen=1 %to &&num_scenarios_&seg..;

         /* Filter the allocation table to this segment and this scenario. */
         data _tmp_alloc_filtered_&seg._&scen.;
            set &ds_in_allocation.;
            &scenario_var.=coalesce(&scenario_var., "&&segment_scenario_&seg._&scen..");
            if &&segment_filter_&seg.. and &scenario_var. in ("&&segment_scenario_&seg._&scen.." "") then output;
         run;

         /* If there are no allocations for this segment, create a dummy allocation table that has 1 value and has an allocation weight of 1. This means we'll do no allocation within this segment */
         %let alloc_vars_flag=Y;
         %if ( %rsk_attrn(WORK._tmp_alloc_filtered_&seg._&scen., nobs) = 0) %then %do;
            %let alloc_vars_flag=N;
            %let alloc_by_vars = __tmp_alloc_var__;

            data _tmp_alloc_filtered_&seg._&scen.;
               if _n_=0 then set _tmp_alloc_filtered_&seg._&scen.;
               __tmp_alloc_var__="__temp_alloc_value__";
               &percentage_var.=1;
               &scenario_var.="&&segment_scenario_&seg._&scen..";
            run;

         %end;
         %else %if (%sysevalf(%superq(alloc_by_vars) eq, boolean)) %then %do;
            %put ERROR: alloc_by_vars is required if ds_in_allocation is provided.;
            %return;
         %end;

         /* Create different macrovar forms of the allocation variables for use through this segment loop */
         %let unquoted_csv_allocvar_list = %sysfunc(prxchange(s/\s+/%str(, )/i, -1, &alloc_by_vars.));
         %let quoted_csv_allocvar_list = %sysfunc(prxchange(s/(\w+)/"$1"/i, -1, %bquote(&unquoted_csv_allocvar_list.)));
         %let sql_a_csv_allocvar_list = %sysfunc(prxchange(s/(\w+)/a.$1/i, -1, %bquote(&unquoted_csv_allocvar_list.)));
         %let sql_b_csv_allocvar_list = %sysfunc(prxchange(s/(\w+)/b.$1/i, -1, %bquote(&unquoted_csv_allocvar_list.)));
         %let sql_join_csv_allocvar_list = %sysfunc(prxchange(s/(\w+)/a.$1=b.$1/i, -1, %bquote(&alloc_by_vars.)));
         %let sql_join_csv_allocvar_list = %sysfunc(prxchange(s/(\s+)/ and /i, -1, %bquote(&sql_join_csv_allocvar_list.)));

         /* Filter the input sampling frame to this segment.  Add the dummy allocation variable if needed */
         data &samp_frame_libref.._tmp_samp_frame_filtered_&seg._&scen.;
            set &inSamplingFrame. (where=(&&segment_filter_&seg..));
            %if &alloc_vars_flag.=N %then %do;
               __tmp_alloc_var__ = "__temp_alloc_value__";
            %end;
         run;

         %if %rsk_attrn(&samp_frame_libref.._tmp_samp_frame_filtered_&seg._&scen., nobs) = 0 %then %do;
            %put WARNING: No observations were found in the sampling frame for segment filter: &&segment_filter_&seg..;
            %put WARNING: Skipping sampling for this segment;
            %goto continue;
         %end;

         %if "&samp_frame_engine." = "V9" %then %do;
            proc sort data=&samp_frame_libref.._tmp_samp_frame_filtered_&seg._&scen.; by &alloc_by_vars.; run;
         %end;

         /* Get the starting count, sum, and mean of the target var for each allocation segment
         We use this to estimate how many samples need generated for this segment to meet its projections. */
         proc summary data = &samp_frame_libref.._tmp_samp_frame_filtered_&seg._&scen. missing nway;
            class &alloc_by_vars.;
            var &target_var.;
            output
               out = _tmp_segment_summary_&seg._&scen._1
               sum(&target_var.) = _computed_sum_
               mean(&target_var.) = _computed_mean_
               ;
         run;

         /* Filter the projections for this segment and scenario and add on the segment's allocation weights. */
         proc sql;
            create table _tmp_proj_w_alloc_&seg._&scen._1 as
            select a.*, &sql_b_csv_allocvar_list, coalesce(b.&percentage_var., 1) as &percentage_var.
            from _tmp_proj_srt_ (where=(
                  &segmentFilterVar.="&&segment_filter_&seg.."
                  and &scenario_var. in ("&&segment_scenario_&seg._&scen.." "")
               ) ) as a
            left join _tmp_alloc_filtered_&seg._&scen. as b
            on a.&scenario_var.=b.&scenario_var.
            order by &sql_b_csv_allocvar_list., a.&scenario_var., a.&horizon_var.
            ;
         quit;

         /* Generation loop: Randomly draw samples from the input sampling frame to meet projections for this segment and scenario.   We repeat the drawing until either:
            1. The target projections for this segment and scenario are reached, or
            2. We've repeated this segment's sampling &max_sampling_reps. times, in which case we stop sampling and throw a warning that projections were not reached for this segment and scenario.
         */
         %let targets_reached=0;
         %let rep=0;
         %do %while (&targets_reached.=0 and &rep.<&max_sampling_reps.);

            %let rep=%eval(&rep+1);
            %put Note: Segment: &seg.., Scenario: &&segment_scenario_&seg._&scen., Sample Replication: &rep..;

            /* Determine the amount of samples to take from each allocation segment
               Also, add lookup indexes for this segment's allocation by-groups, scenario, and horizons */
            data _tmp_proj_w_growth_&seg._&scen._&rep. (drop=_rc_ _computed_mean_);
               set _tmp_proj_w_alloc_&seg._&scen._&rep. end=last;
               by &alloc_by_vars. &scenario_var. &horizon_var.;
               retain _alloc_bygroup_index_ _scenario_index_ _horizon_index_ 0;

               if _N_ = 1 then do;
                  declare hash hMean(dataset: "_tmp_segment_summary_&seg._&scen._&rep.");
                  hMean.defineKey(&quoted_csv_allocvar_list.);
                  hMean.defineData("_computed_mean_");
                  hMean.defineDone();
               end;

               call missing(_computed_mean_);
               _rc_=hMean.find();

               /* Determine the amount of growth (relativeValueAlloc) and number of samples (_NSIZE_)
               needed for this scenario+horizon+allocation segment */
               relativeValueAlloc=abs(&relativeValue_var.)*&percentage_var.;
               _NSIZE_=ceil(%sysevalf(&simulation_factor.*&rep.) * ceil(relativeValueAlloc / _computed_mean_));

               /* Set the allocation segment, scenario, and horizon lookup indexes */
               if first.%scan(&alloc_by_vars., -1, %str( )) then do;
                  _alloc_bygroup_index_+1;
                  _scenario_index_=&scen.;
                  _horizon_index_=1;
               end;
               else if first.&scenario_var. then do;
                  _scenario_index_+1;
                  _horizon_index_=1;
               end;
               else if first.&horizon_var. then do;
                  _horizon_index_+1;
               end;

               if last.%scan(&alloc_by_vars., -1, %str( )) then
                  call symputx(catx("_", "scenario_cnt", put(_alloc_bygroup_index_, 8.)), _scenario_index_, "L");
               if last.&scenario_var. then
                  call symputx(catx("_", "horizon_cnt", put(_alloc_bygroup_index_, 8.), put(_scenario_index_, 8.) ), _horizon_index_, "L");
               if last then
                  call symputx("alloc_bygroup_cnt", _alloc_bygroup_index_, "L");

            run;

            /* Get the total amount of growth needed across all horizons for this scenario (sum over all horizons within each allocation segment and scenario)
               -We try to draw enough growth (samples) within each allocation segment to reach the target growth for all of that allocation segment's horizons in total for a given scenario. The total growth drawn is then delegated into each horizon after sampling.
               -For elimination we're sampling without replacement, so make sure the number of samples we take is not more than the the number of observations for the segment */
            proc sql noprint;
               create table _tmp_nsize_by_alloc_seg_&seg._&scen._&rep. as
               select distinct &sql_a_csv_allocvar_list.
                  , min(coalesce(sum(b._NSIZE_), 0), a._FREQ_) as _NSIZE_
                  , min(coalesce(sum(b.relativeValueAlloc), 0), a._computed_sum_) as relativeValueAlloc
                  , case
                     when a._FREQ_ = min(coalesce(sum(b._NSIZE_), 0), a._FREQ_) then 1
                     else 0
                     end as all_obs_elim_flag
               from _tmp_segment_summary_&seg._&scen._&rep. as a left join _tmp_proj_w_growth_&seg._&scen._&rep. as b
               on &sql_join_csv_allocvar_list.
               group by &sql_a_csv_allocvar_list.
               ;
            quit;

            /* SURVEYSELECT can have issues when the DATA= table is a CAS table but the SAMPSIZE table is a SAS dataset.  So to avoid, move the SAMPSIZE dataset into CAS (if the DATA= table is a CAS table) */
            %let sampsize_table = work._tmp_nsize_by_alloc_seg_&seg._&scen._&rep.;
            %if "&samp_frame_engine." = "CAS" %then %do;
               data &samp_frame_libref.._tmp_nsize_by_alloc_seg_&seg._&scen._&rep.;
                  set work._tmp_nsize_by_alloc_seg_&seg._&scen._&rep.;
               run;
               %let sampsize_table = &samp_frame_libref.._tmp_nsize_by_alloc_seg_&seg._&scen._&rep.;
            %end;

            %if %upcase(&debug.) eq TRUE %then %do;
               %put Segment: &&segment_filter_&seg..;
               %put Replication: &rep.;
               %put Number of allocation variable by groups: &alloc_bygroup_cnt.;
               %do b=1 %to &alloc_bygroup_cnt.;
                  %put Number of scenarios in by-group &b.: &&scenario_cnt_&b..;
                  %do s=1 %to &&scenario_cnt_&b..;
                     %put     Horizons in scenario &s.: &&horizon_cnt_&b._&s..;
                  %end;
               %end;
            %end;

            /* Draw from the sampling frame.  The number of draws for each allocation segment (strata) is specified in
            _tmp_nsize_by_alloc_seg_&seg._&scen._&rep. (_NSIZE_).
               Ex: currency="USD" --> _NSIZE_=50

               For elimination, we draw separately for each scenario and then combine the sampling results.
               This is because it is valid for the same instrument to be dropped once in each scenario
               (Ex: instid CI_00001 could be dropped in the Adverse scenario at h=1 and
               in the Basecase scenario at h=2.)
            */
            proc surveyselect noprint
               data    =   &samp_frame_libref.._tmp_samp_frame_filtered_&seg._&scen.
               method  =   SRS    /* ELIM: random sampling without replacement */
               reps    =   1                    /* sample only 1 time */
               /* how many samples to take for each allocation segment */
               sampsize=&sampsize_table. (drop=relativeValueAlloc all_obs_elim_flag)
               %if (%sysevalf(%superq(custom_seed) ne, boolean)) %then %do;
                  seed = %eval(&custom_seed.+&rep.-1)   /* ensure we use a different seed each rep to avoid getting stuck with the same draws */
               %end;
               out = _tmp_out_sample_&seg._&scen._&rep. (drop = replicate SamplingWeight SelectionProb);
               strata &alloc_by_vars.;
            run;

            proc sort data=_tmp_out_sample_&seg._&scen._&rep.; by &alloc_by_vars.; run;

            /* Delegate the segment's samples into each scenario+horizon based on that scenario+horizon's growth (relativeValue) */
            %let next_rep=%eval(&rep+1);
            data
               _tmp_out_sample_selected_&seg._&scen._&rep. (
                  drop=
                     relativeValueAlloc _rc_ _tmp_cum_target_value_ _segment_targets_reached_ _alloc_bygroup_index_
                     _scenario_index_ _horizon_index_ &relativeValue_var. &percentage_var. __inst_cnt__ all_obs_elim_flag
                     %if &alloc_vars_flag.=N %then %do;
                        __tmp_alloc_var__
                     %end;
                     rename = (&scenario_var=_inst_scenario_name_ &horizon_var.=_inst_scenario_forecast_time_)
               )
               _tmp_proj_w_alloc_&seg._&scen._&next_rep. (keep=&alloc_by_vars. &scenario_var. &horizon_var. relativeValueAlloc &relativeValue_var. &percentage_var.);

               set  _tmp_out_sample_&seg._&scen._&rep.;

               by &alloc_by_vars.;
               retain _tmp_cum_target_value_ _segment_targets_reached_ _alloc_bygroup_index_ _scenario_index_ _horizon_index_ __inst_cnt__ 0;

               if _N_=0 then do;
                  set _tmp_proj_w_growth_&seg._&scen._&rep. (keep=_alloc_bygroup_index_ _scenario_index_ _horizon_index_ relativeValueAlloc &scenario_var. &horizon_var.);
                  set _tmp_nsize_by_alloc_seg_&seg._&scen._&rep. (keep=&alloc_by_vars. all_obs_elim_flag);
               end;

               if _N_ = 1 then do;

                  declare hash hTarget(dataset: "_tmp_proj_w_growth_&seg._&scen._&rep.");
                  hTarget.defineKey("_alloc_bygroup_index_", "_scenario_index_", "_horizon_index_");
                  hTarget.defineData("relativeValueAlloc", "&scenario_var.", "&horizon_var.");
                  hTarget.defineDone();

                  declare hash hAllObs(dataset: "_tmp_nsize_by_alloc_seg_&seg._&scen._&rep.");
                  hAllObs.defineKey(&quoted_csv_allocvar_list.);
                  hAllObs.defineData("all_obs_elim_flag");
                  hAllObs.defineDone();

               end;

               /* If this is the first observation for an allocation segment, reset */
               if first.%scan(&alloc_by_vars., -1, %str( )) then do;
                  _tmp_cum_target_value_=0;
                  _segment_targets_reached_=0;
                  _alloc_bygroup_index_+1;
                  _scenario_index_=&scen.;
                  _horizon_index_=1;
               end;

               /* Only output rows if we haven't reached the target projections for every scenario and horizon for this allocation segment */
               if _segment_targets_reached_=0 then do;

                  /* Lookup the target projection for the given allocation segment, scenario, and horizon */
                  call missing(relativeValueAlloc, &scenario_var., &horizon_var.);
                  _rc_ = hTarget.find();

                  _tmp_cum_target_value_ + &target_var.;
                  __inst_cnt__ + 1;

                  output _tmp_out_sample_selected_&seg._&scen._&rep.;

                  /* if this is the last observation in the allocation segment and we still haven't reached the target growth for this allocation segment, output the remaining growth (for this and all remaining scenarios/horizons) to _tmp_proj_w_alloc_ for the next replication. */
                  if last.%scan(&alloc_by_vars., -1, %str( )) then do;

                     /* See if all observations have been eliminated for this allocation segment and scenario.  If so, do not output any growth for the next replication, since there are no more instruments to draw. */
                     call missing(all_obs_elim_flag);
                     _rc_ = hAllObs.find();
                     if all_obs_elim_flag=1  then do;
                        put "WARNING: All observations have been eliminated for segment with filter: &&segment_filter_&seg.. for scenario: " &scenario_var.;
                        %if &alloc_vars_flag.=Y %then %do;
                           put "WARNING: and allocation segment values: " %sysfunc(prxchange(s/(\w+)/$1= /, -1, &alloc_by_vars.));
                        %end;
                        put "WARNING: Targets may not have been reached for this horizon (horizon=" &horizon_var. ") and horizons after this one.";
                        return;
                     end;

                     &percentage_var. = 1;

                     /* For the current scenario and horizon, we still need to hit the expected growth projection (relativeValueAlloc) minus the growth we've accumulated so far (_tmp_cum_target_value_) */
                     if _tmp_cum_target_value_ < coalesce(relativeValueAlloc*(1-%sysfunc(coalescec(&projection_epsilon., 0))), 0) then do;
                        &relativeValue_var. = sum(relativeValueAlloc, -_tmp_cum_target_value_);
                        output _tmp_proj_w_alloc_&seg._&scen._&next_rep.;
                     end;

                     /* The scenarios and horizons beyond this one haven't received any new samples yet, so they still need
                     to hit the full expected growth projection they had before (relativeValueAlloc) */
                     do while ( _horizon_index_ < symgetn(catx("_", "horizon_cnt", put(_alloc_bygroup_index_, 8.), put(_scenario_index_, 8.))) );
                        _horizon_index_ + 1;
                        call missing(relativeValueAlloc, &scenario_var., &horizon_var.);
                        _rc_ = hTarget.find();
                        &relativeValue_var.=relativeValueAlloc;
                        output _tmp_proj_w_alloc_&seg._&scen._&next_rep.;
                     end;

                  end;
                  else if(_tmp_cum_target_value_ >= relativeValueAlloc*(1-%sysfunc(coalescec(&projection_epsilon., 0)))) then do;

                     /* This is not the last observation in the allocation segment and we've reached/exceeded the required target amount for the given scenario/horizon.  Increment the scenario/horizon index. */
                     _tmp_cum_target_value_ = 0;
                     _horizon_index_ + 1;
                     if( _horizon_index_ > symgetn(catx("_", "horizon_cnt", put(_alloc_bygroup_index_, 8.), put(_scenario_index_, 8.))) ) then do;
                        _segment_targets_reached_=1;  /* We've reached targets for all scenarios+horizons for this segment */
                     end;

                  end;

               end; /* end if _segment_targets_reached_=0 */
            run;


            /* If all targets were reached, stop the loop for this scenario (no more sampling) */
            %if ( %rsk_attrn(WORK._tmp_proj_w_alloc_&seg._&scen._&next_rep., nobs) = 0) %then
               %let targets_reached=1;
            %else %if &rep.=&max_sampling_reps. %then %do;
               %put WARNING: Some horizon target projections were not reached for segment with filter: &&segment_filter_&seg.. for scenario: &&segment_scenario_&seg._&scen...;
               %put WARNING: Increase simulation_factor to ensure targets are reached.;
               data _targets_not_reached_&seg._&scen.;
                  set _tmp_proj_w_alloc_&seg._&scen._&next_rep.;
               run;
            %end;
            %else %do;
               /* Remove the selected observations from this segment's sampling frame so that
               we don't draw them again for this segment in the next replication. */

               %if "&samp_frame_engine." = "CAS" %then %do;
                  data &samp_frame_libref.._tmp_out_sample_selected_ids_ (rename=(orig_&id_var. = &id_var.));
                     length orig_&id_var. varchar(%sysfunc(coalescec(&id_var_length.,*)));
                     set _tmp_out_sample_selected_&seg._&scen._&rep. (keep=&id_var.);
                     orig_&id_var. = strip(&id_var.);
                     drop &id_var.;
                  run;
                  data &samp_frame_libref.._tmp_samp_frame_filtered_&seg._&scen.;
                     merge &samp_frame_libref.._tmp_samp_frame_filtered_&seg._&scen. (in=a)
                           &samp_frame_libref.._tmp_out_sample_selected_ids_ (in=b);
                     by &id_var.;
                     if not b;
                  run;
               %end;
               %else %do;
                  proc sql undo_policy = none;
                     create table &samp_frame_libref.._tmp_samp_frame_filtered_&seg._&scen. as
                     select a.*
                     from &samp_frame_libref.._tmp_samp_frame_filtered_&seg._&scen. as a
                     left join _tmp_out_sample_selected_&seg._&scen._&rep. (keep=&id_var.) as b
                     on a.&id_var.=b.&id_var.
                     where b.&id_var. is missing
                     order by &sql_a_csv_allocvar_list.
                     ;
                  quit;
               %end;

               /* Get the new count, sum, and mean of the target var for each allocation segment */
               proc summary data = &samp_frame_libref.._tmp_samp_frame_filtered_&seg._&scen. missing nway;
                  class &alloc_by_vars.;
                  var &target_var.;
                  output
                     out = _tmp_segment_summary_&seg._&scen._&next_rep.
                     sum(&target_var.) = _computed_sum_
                     mean(&target_var.) = _computed_mean_
                     ;
               run;

            %end; /* End targets not reached */

            %if &rep.=1 %then %do;
               data _tmp_out_sample_selected_&seg._&scen.; set _tmp_out_sample_selected_&seg._&scen._&rep.; run;
            %end;
            %else %do;
               proc append base=_tmp_out_sample_selected_&seg._&scen. data=_tmp_out_sample_selected_&seg._&scen._&rep.; run;
            %end;

         %end; /* End generation loop (until targets reached) for a given segment */

         %if &scen.=1 %then %do;
            data _tmp_out_sample_selected_&seg.; set _tmp_out_sample_selected_&seg._&scen.; run;
         %end;
         %else %do;
            proc append base=_tmp_out_sample_selected_&seg. data=_tmp_out_sample_selected_&seg._&scen.; run;
         %end;

         /* Clean-up */
         %if %upcase(&debug.) ne TRUE %then %do;
            proc datasets library = work nolist nodetails nowarn;
               delete
                  _tmp_alloc_filtered_&seg._&scen.:
                  _tmp_segment_summary_&seg._&scen.:
                  _tmp_nsize_by_alloc_seg_&seg._&scen.:
                  _tmp_out_sample_selected_&seg._&scen.:
                  _tmp_samp_frame_filtered_&seg._&scen.:
                  _tmp_proj_w_alloc_&seg._&scen.:
                  _tmp_proj_w_growth_&seg._&scen.:
                  _tmp_out_sample_&seg._&scen.:
                  ;
            quit;
            %if "&samp_frame_engine." = "CAS" %then %do;
               proc datasets library = &samp_frame_libref. nolist nodetails nowarn;
                  delete
                     _tmp_nsize_by_alloc_seg:
                     _tmp_samp_frame_filtered:
                     _tmp_out_sample_selected_ids_
                  ;
               quit;
            %end;
         %end;

      %end; /* End for each segment scenario loop */

      /* Write the sampling observations to the output table */
      %if %rsk_dsexist(&outSample.)=0 %then %do;
         data &outSample.; set _tmp_out_sample_selected_&seg.; run;
      %end;
      %else %do;
         proc append base=&outSample. data=_tmp_out_sample_selected_&seg.; run;
      %end;

      %continue:

      /* Clean-up */
      %if %upcase(&debug.) ne TRUE %then %do;
         proc datasets library = work nolist nodetails nowarn;
            delete
               _tmp_alloc_filtered_&seg.:
               _tmp_segment_summary_&seg.:
               _tmp_nsize_by_alloc_seg_&seg.:
               _tmp_out_sample_selected_&seg.:
               _tmp_samp_frame_filtered_&seg.:
               _tmp_proj_w_alloc_&seg.:
               _tmp_proj_w_growth_&seg.:
               _tmp_out_sample_&seg.:
               ;
         quit;
      %end;

   %end; /* End for each segment loop */

   /* Clean-up */
   %if %upcase(&debug.) ne TRUE %then %do;
      proc datasets library = work nolist nodetails nowarn;
         delete
            __tmp_alloc_ds__
            _tmp_proj_srt_
            ;
      quit;
   %end;

%mend;