%macro corew_create_audit_tables(solution =
                              , host =
                              , port =
                              , logonHost =
                              , logonPort =
                              , username =
                              , password =
                              , authMethod = bearer
                              , client_id =
                              , client_secret =
                              , cycleKey =
                              , auditScriptId =
                              , cycleEntity =
                              , outDsCycleTasksSummary = audit_cycle_tasks_summary
                              , outDsCycleTasksHistory = audit_cycle_tasks_history
                              , outVarToken = accessToken
                              , debug = false
                              );

   %local wfTemplateKey num_analysis_runs i processId;

   %if(%sysevalf(%superq(cycleKey) eq, boolean)) %then %do;
      %put ERROR: cycleKey is required.;
      %abort;
   %end;


   /**************************************************/
   /*********GET THE REQUIRED AUDIT RESOURCES*********/
   /**************************************************/

   /************CYCLE************/
   %core_rest_get_cycle(solution = &solution.
                     , host = &host.
                     , port = &port.
                     , logonHost = &logonHost.
                     , logonPort = &logonPort.
                     , username = &username.
                     , password = &password.
                     , authMethod = &authMethod.
                     , client_id = &client_id.
                     , client_secret = &client_secret.
                     , key = &cycleKey.
                     , outds = _tmp_cycle_summary_
                     , outds_comments = _tmp_cycle_comments_
                     , outds_attachments = _tmp_cycle_attachments_
                     , debug = &debug.
                     );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the cycle with key &cycleKey.;
      %abort;
   %end;

   /* Exit if no cycle was found */
   %if not %rsk_dsexist(work._tmp_cycle_summary_) or %rsk_attrn(work._tmp_cycle_summary_, nobs) = 0 %then %do;
      %put ERROR: Failed to find a cycle with key &cycleKey. in solution &solution.;
      %abort;
   %end;

   /*********CYCLE'S ATTACHMENT HISTORY**********/
   %core_rest_get_attachment_history(solution = &solution.
                                 , host = &host.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , objectType = cycles
                                 , objectKey = &cycleKey.
                                 , outds_history = _tmp_attachment_history_
                                 , outds_deleted = _tmp_attachment_deleted_history_
                                 , debug = &debug.
                                 );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the cycle attachments (key &cycleKey.);
      %abort;
   %end;

   /*********CYCLE'S WF TEMPLATE**********/
   %core_rest_get_link_instances(solution = &solution.
                              , host = &host.
                              , port = &port.
                              , logonHost = &logonHost.
                              , logonPort = &logonPort.
                              , username = &username.
                              , password = &password.
                              , authMethod = &authMethod.
                              , client_id = &client_id.
                              , client_secret = &client_secret.
                              , objectType = cycles
                              , objectKey = &cycleKey.
                              , linkType = wfTemplate_cycle
                              , outds = _tmp_cycle_wftemp_link_insts_
                              , debug = &debug.
                              );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the linked workflow template for cycle &cycleKey.;
      %abort;
   %end;

   /* Exit if no workflow template was found */
   %if not %rsk_dsexist(work._tmp_cycle_wftemp_link_insts_) or %rsk_attrn(work._tmp_cycle_wftemp_link_insts_, nobs) = 0 %then %do;
      %put ERROR: Failed to find a linked workflow template for cycle &cycleKey. in solution &solution.;
      %abort;
   %end;

   data _null_;
      set _tmp_cycle_wftemp_link_insts_;
      call symputx("wfTemplateKey", businessObject1, "L");
   run;

   %core_rest_get_wftemplate(solution = &solution.
                           , host = &host.
                           , port = &port.
                           , logonHost = &logonHost.
                           , logonPort = &logonPort.
                           , username = &username.
                           , password = &password.
                           , authMethod = &authMethod.
                           , client_id = &client_id.
                           , client_secret = &client_secret.
                           , key = &wfTemplateKey.
                           , outds = _tmp_wftemplate_summary_
                           , outds_tasks = _tmp_wftemplate_tasks_
                           , outds_task_scripts = _tmp_wftemplate_task_scripts_
                           , debug = &debug.
                           );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the workflow template with key &wfTemplateKey.;
      %abort;
   %end;

   /* Exit if no workflow template was found */
   %if not %rsk_dsexist(work._tmp_wftemplate_summary_) or %rsk_attrn(work._tmp_wftemplate_summary_, nobs) = 0 %then %do;
      %put ERROR: Failed to find workflow template &wfTemplateKey. in solution &solution.;
      %abort;
   %end;

   /*****CYCLE'S WF PROCESS HISTORY******/
   %core_rest_get_wf_process_history(solution = &solution.
                                    , host = &host.
                                    , port = &port.
                                    , logonHost = &logonHost.
                                    , logonPort = &logonPort.
                                    , username = &username.
                                    , password = &password.
                                    , authMethod = &authMethod.
                                    , client_id = &client_id.
                                    , client_secret = &client_secret.
                                    , objectType = cycles
                                    , objectKey = &cycleKey.
                                    , processId =
                                    , outds_process = wf_process
                                    , outds_process_history =
                                    , outds_tasks =
                                    , outds_tasks_history =
                                    , debug = &debug.
                                 );

   /* Get the "active" processId if there is one.  Otherwise, get the most recently created processId */
   /* Note: generally, a cycle should only have one processId total, but this is just in case */
   proc sort data=wf_process; by descending createdTimeStamp; run;
   data _null_;
      set wf_process;
      if _n_=1 then
         call symputx("processId", id, "L");
      if state ne "Completed" then do;
         call symputx("processId", id, "L");
         stop;
      end;
   run;

   %if "&processId." = "" %then %do;
      %put ERROR: No historical or active workflow processes were found for cycle with key &cycleKey..  Please ensure that the workflow for the cycle is started and that the workflow history service is up.;
      %abort;
   %end;

   %core_rest_get_wf_process_history(solution = &solution.
                                    , host = &host.
                                    , port = &port.
                                    , logonHost = &logonHost.
                                    , logonPort = &logonPort.
                                    , username = &username.
                                    , password = &password.
                                    , authMethod = &authMethod.
                                    , client_id = &client_id.
                                    , client_secret = &client_secret.
                                    , objectType = cycles
                                    , objectKey = &cycleKey.
                                    , processId = &processId.
                                    , userTasksOnly = Y
                                    , outds_process = wf_process
                                    , outds_process_history = wf_process_history
                                    , outds_tasks = wf_process_tasks
                                    , outds_tasks_history = wf_process_tasks_history
                                    , debug = &debug.
                                 );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the workflow process history for process ID &processId. (cycle &cycleKey.);
      %abort;
   %end;

   /********CYCLE'S ANALYSIS RUNS********/
   %core_rest_get_link_instances(solution = &solution.
                              , host = &host.
                              , port = &port.
                              , logonHost = &logonHost.
                              , logonPort = &logonPort.
                              , username = &username.
                              , password = &password.
                              , authMethod = &authMethod.
                              , client_id = &client_id.
                              , client_secret = &client_secret.
                              , objectType = cycles
                              , objectKey = &cycleKey.
                              , linkType = analysisRun_cycle
                              , outds = _tmp_cycle_ar_link_insts_
                              , debug = &debug.
                              );

   /* Exit in case of errors */
   %if(not &httpSuccess.) %then %do;
      %put ERROR: Failed to get the analysis runs linked to cycle &cycleKey.;
      %abort;
   %end;

   data _null_;
      set _tmp_cycle_ar_link_insts_ end=last;
      call symputx(catt("analysis_run_key_", _N_), businessObject1, "L");
      if last then
         call symputx("num_analysis_runs", _N_, "L");
   run;

   %do i=1 %to &num_analysis_runs.;

      /* Get each analysis run and its parameters */
      %core_rest_get_analysis_run(solution = &solution.
                                 , host = &host.
                                 , port = &port.
                                 , logonHost = &logonHost.
                                 , logonPort = &logonPort.
                                 , username = &username.
                                 , password = &password.
                                 , authMethod = &authMethod.
                                 , client_id = &client_id.
                                 , client_secret = &client_secret.
                                 , key = &&&analysis_run_key_&i..
                                 , outds = _tmp_analysis_run_
                                 , outds_params = _tmp_analysis_run_params_
                                 , debug = &debug.
                                 );

      /* Exit in case of errors */
      %if (not &httpSuccess.) or %rsk_attrn(work._tmp_analysis_run_, nobs) = 0 %then %do;
         %put ERROR: Failed to get analysis run &&&analysis_run_key_&i..;
         %abort;
      %end;

      data _tmp_analysis_run_;
         length uri $2000;
         set _tmp_analysis_run_;
         uri=catt("%sysget(SAS_SERVICES_URL)/SASRiskCirrus/Solutions/", createdInTag, "/#/AnalysisRun/edit/key/", key);
      run;

      %if &i.=1 %then %do;
         data analysis_runs; set _tmp_analysis_run_; run;
         data analysis_run_params; set _tmp_analysis_run_params_; run;
      %end;
      %else %do;
         proc append base=analysis_runs data=_tmp_analysis_run_; run;
         proc append base=analysis_run_params data=_tmp_analysis_run_params_; run;
      %end;

   %end;


   /*********************************************/
   /*********PREPARE THE RETRIEVED DATA *********/
   /*********************************************/

   /* Update the workflow template scripts table (script information for each type=script task) */
   data _tmp_wftemplate_task_scripts_;
      set _tmp_wftemplate_task_scripts_(
         keep=taskId objectId name
         rename=(objectId=scriptId name=scriptName)
      );
   run;

   /* Update the workflow process task history table (historical task info - entry for each process task event)
   /* Add the process task history order and task completed time to each row:
         -task order
         -completed time: Each task's start time is its state="Started" entry.  Each task's end time is the next task's state="Started" entry. */
   proc sort data=wf_process_tasks_history; by createdTimeStamp; run;
   data wf_process_tasks_hist_filtered;
      set wf_process_tasks_history (rename=(createdTimeStamp=taskCreatedTimeStamp state=taskState taskId=taskHistoryId) where=(taskState="Started") drop=active);
      taskOrder=_N_;
   run;
   data wf_process_tasks_hist_filtered;
      merge wf_process_tasks_hist_filtered(firstobs=1)
            wf_process_tasks_hist_filtered(firstobs=2 keep=taskCreatedTimeStamp rename=(taskCreatedTimeStamp=taskCompletedTimeStamp));
   run;

   /* Update the workflow process tasks table (task info - entry for each task) */
   proc sort data=wf_process_tasks; by createdTimeStamp; run;
   data wf_process_tasks;
      merge wf_process_tasks(firstobs=1)
            wf_process_tasks(firstobs=2 keep=createdTimeStamp rename=(createdTimeStamp=completedTimeStamp));

      /* Calculate how long each task took */
      /* Note: we have to calculate this ourselves - workflow has the "duration" variable for each task, which is the
      time it took for the task state to go from "Started" to "Completed".  In a Cirrus cycle, when a user executes a script for a task, the
      state is set to "Completed" before the analysis run is created/run, so the "duration" does not capture the analysis run time for the task. */
      if completedTimeStamp="" then
         taskHistoryTime=max(datetime()-input(createdTimeStamp,e8601dz25.), 0);
      else
         taskHistoryTime=input(completedTimeStamp,e8601dz25.)-input(createdTimeStamp,e8601dz25.);
   run;

   /* Calculate the total time each task took (in case it was performed multiple times) */
   proc sql;
      create table wf_process_tasks_hist_updated as
      select id as taskHistoryId
            , name as taskName
            , actualOwner as taskActualUser
            , parentId as taskProcessId
            , state as taskStatus
            , createdTimeStamp
            , sum(taskHistoryTime) as taskHistoryTime
      from wf_process_tasks
      group by taskName
      order by taskName, createdTimeStamp
      ;
   quit;

   data wf_process_tasks_hist_updated (drop=createdTimeStamp taskHistoryTime);
      length taskActualDuration $200;
      set wf_process_tasks_hist_updated;
      by taskName createdTimeStamp;

      days=taskHistoryTime/(60*60*24);
      hours=(days-floor(days))*24;
      mins=(hours-floor(hours))*60;
      secs=(mins-floor(mins))*60;

      taskActualDuration=cat(floor(days), "d ", floor(hours), "h ", floor(mins), "m ", floor(secs), "s ");
      drop days hours mins secs;
      taskActualDuration=prxchange('s/(\d+)\s+(\w)\w+/$1$2 /i', -1, taskActualDuration);
      taskActualDuration=prxchange('s/(^|\s+)0\w//i', -1, taskActualDuration);

      if last.taskName then taskLatestEntryFlag=1;    /* Capture the latest entry for each task, since a task may have been performed multiple times */
      else taskLatestEntryFlag=0;
   run;


   /* Update the process history table:
      1. Only include the process events of interest (ex: only when the CIRRUS_WORKFLOW_TRANSITIONS is changed)
      2. Add the active taskId and taskName to each process history event if the process event time is between the task start and task complete times
         -this is currently the best guess at the task that triggered this process event
         -do not assign the task to a process event if the process event occurred at the exact same start time as the task start
   */
   data wf_process_history_filtered (
         keep=processId createdTimeStamp state processVariableName processVariableValue createdBy
         rename=(createdTimeStamp=processCreatedTimeStamp state=processState createdBy=processCreatedBy)
      );
      set wf_process_history;
      if state="Variable Updated" then do;
         processVariableName=prxchange('s/^The value of variable "(\w+)".*/$1/i', -1, messageText);
         processVariableValue=prxchange('s/.*"([^"]*)"\.\s*$/$1/i', -1, messageText);
         if processVariableName="CIRRUS_WORKFLOW_TRANSITIONS" and processVariableValue ne "CIRRUS_RUN_SCRIPT" then
            output;
      end;
   run;

   proc sql;
      create table wf_process_history_w_task as
      select a.*, b.taskHistoryId, b.taskName
      from wf_process_history_filtered as a left join wf_process_tasks_hist_filtered as b
      on b.taskCreatedTimeStamp < a.processCreatedTimeStamp <= b.taskCompletedTimeStamp
      ;
   quit;

   /* Update cycle attachments:
      -add taskHistoryId, taskName
      -set each attachment's attachmentStatus to either "Added", "Updated", "Replaced", or "Deleted"
      -mark the task's latest attachment
   */
   proc sort data=_tmp_attachment_history_; by objectId modifiedTimeStamp ; run;
   proc sort data=_tmp_attachment_deleted_history_; by objectId modifiedTimeStamp ; run;

   data attachment_history_all (drop=priorName);

      length attachmentStatus $32 priorName attachmentPriorName $200;
      set   _tmp_attachment_history_ (in=history)
            _tmp_attachment_deleted_history_ (in=deleted);
      by objectId modifiedTimeStamp;
      retain priorName;

      attachmentPriorName="";
      if first.objectId then attachmentStatus="Added";
      else do;
         if deleted then attachmentStatus="Deleted";
         else do;
            if name=priorName then
               attachmentStatus="Updated";
            else do;
               attachmentStatus="Replaced";
               attachmentPriorName=priorName;
            end;
         end;
      end;
      priorName=name;

   run;

   proc sql;
      create table cycle_attachements_w_task as
      select a.description as attachmentDescription
            , catt("%sysget(SAS_SERVICES_URL)", a.uri) as attachmentUri
            , a.name as attachmentName
            , a.createdBy as attachmentUser
            , a.modifiedTimeStamp as attachmentTime
            , a.attachmentStatus
            , a.attachmentPriorName
            , b.taskHistoryId
            , b.taskName
            , case
               when a.modifiedTimeStamp = max(a.modifiedTimeStamp) then 1
               else 0
            end as taskLatestEntryFlag
      from attachment_history_all as a left join wf_process_tasks_hist_filtered as b
      on (b.taskCreatedTimeStamp <= a.modifiedTimeStamp <= b.taskCompletedTimeStamp)
         or (b.taskCreatedTimeStamp <= a.modifiedTimeStamp and missing(b.taskCompletedTimeStamp))
      group by b.taskName
      order by a.modifiedTimeStamp
      ;
   quit;

   /* Cycle comments - add taskHistoryId, taskName, and mark the task's latest comment */
   proc sql;
      create table cycle_comments_w_task as
      select a.commentText
            , a.modifiedBy as commentUser
            , a.modifiedTimeStamp as commentTime
            , b.taskHistoryId
            , b.taskName
            , case
               when a.modifiedTimeStamp = max(a.modifiedTimeStamp) then 1
               else 0
            end as taskLatestEntryFlag
      from _tmp_cycle_comments_ as a left join wf_process_tasks_hist_filtered as b
      on (b.taskCreatedTimeStamp <= a.modifiedTimeStamp <= b.taskCompletedTimeStamp)
         or (b.taskCreatedTimeStamp <= a.modifiedTimeStamp and missing(b.taskCompletedTimeStamp))
      group by b.taskName
      ;
   quit;

   /* Analysis Runs - add taskHistoryId, taskName, and mark the task's latest analysis run */
   proc sql;
      create table analysis_runs_w_task as
      select a.createdBy as analysisRunCreatedBy
            , a.statusCd as analysisRunStatus
            , a.name as analysisRunName
            , a.creationTimeStamp as analysisRunStartTime
            , a.modifiedTimeStamp as analysisRunEndTime
            , a.uri as analysisRunUri
            , b.taskHistoryId
            , b.taskName
            , case
               when a.creationTimeStamp = max(a.creationTimeStamp) then 1
               else 0
            end as taskLatestEntryFlag
      from analysis_runs as a left join wf_process_tasks_hist_filtered as b
      on a.userTaskName=b.taskName and
         ( (b.taskCreatedTimeStamp <= a.creationTimeStamp <= b.taskCompletedTimeStamp)
         or (b.taskCreatedTimeStamp <= a.creationTimeStamp and missing(b.taskCompletedTimeStamp)) )
      group by b.taskName
      order by b.taskName, a.creationTimeStamp
      ;
   quit;


   /********************************************************************/
   /******************CREATE THE OUTPUT AUDIT TABLES********************/
   /********************************************************************/

   /* Create the cycle audit tasks summary table - this is 1 row per each task in the cycle's workflow template with the following information added:
      -the script information for the task (if any)
      -actual task information for completed/in-progress tasks (pulled from the cycle's active process history)
      -cycle attachments for the task (latest only)
      -cycle comments for the task (latest only)
      -cycle analysis runs for the task (latest only)
   */
   %if "&outDsCycleTasksSummary." ne "" %then %do;

      data &outDsCycleTasksSummary. (
         keep =   taskName taskLabel taskCategory taskExpectedUser taskActualUser taskStatus taskExpectedDuration taskActualDuration
                  scriptId scriptName
                  analysisRunName analysisRunStatus analysisRunUri
                  attachmentName attachmentUser attachmentUri
                  commentText commentUser
         );

         length   taskName taskLabel taskCategory taskExpectedUser taskActualUser taskStatus taskExpectedDuration taskActualDuration $200
                  scriptId scriptName $200
                  analysisRunName analysisRunStatus $200 analysisRunUri $2000
                  attachmentName attachmentUser attachmentUri $2000
                  commentText $32000 commentUser $200
         ;

         label taskName="Workflow Task" taskLabel="Task Label" taskCategory="Task Category" taskExpectedUser="Task Expected User" taskActualUser="Task Actual User"
                  taskStatus="Task Status" taskExpectedDuration="Task Expected Duration" taskActualDuration="Task Actual Duration"
               scriptId="Script Id" scriptName="Script Name"
               analysisRunName="Analysis Run Name" analysisRunStatus="Analysis Run Status" analysisRunUri="Analysis Run Uri"
               attachmentName="Attachment Name" attachmentUser="Attachment User" attachmentUri="Attachment Uri"
               commentText="Comment" commentUser="Comment User"
         ;

         set _tmp_wftemplate_tasks_ (
               keep=wfTemplateKey id name label category duration identities
               rename=(id=taskId name=taskName label=taskLabel category=taskCategory identities=taskExpectedUser duration=taskExpectedDuration)
         );

         if _N_=0 then do;
            set _tmp_wftemplate_task_scripts_;
            set wf_process_tasks_hist_updated;
            set cycle_attachements_w_task (drop=attachmentStatus attachmentPriorName);
            set cycle_comments_w_task;
            set analysis_runs_w_task;
         end;

         if _N_=1 then do;

            declare hash hScript(dataset: "_tmp_wftemplate_task_scripts_");
            hScript.defineKey("taskId");
            hScript.defineData("scriptId","scriptName");
            hScript.defineDone();

            declare hash hParent(dataset: "wf_process_tasks_hist_updated (where=(taskLatestEntryFlag=1))");
            hParent.defineKey("taskName");
            hParent.defineData("taskActualUser", "taskActualDuration", "taskStatus");
            hParent.defineDone();

            declare hash hAttach(dataset: "cycle_attachements_w_task (where=(taskLatestEntryFlag=1))");
            hAttach.defineKey("taskName");
            hAttach.defineData("attachmentName", "attachmentUser", "attachmentUri");
            hAttach.defineDone();

            declare hash hComments(dataset: "cycle_comments_w_task (where=(taskLatestEntryFlag=1))");
            hComments.defineKey("taskName");
            hComments.defineData("commentUser", "commentText");
            hComments.defineDone();

            declare hash hAR(dataset: "analysis_runs_w_task (where=(taskLatestEntryFlag=1))");
            hAR.defineKey("taskName");
            hAR.defineData("analysisRunStatus", "analysisRunName", "analysisRunUri");
            hAR.defineDone();

         end;

         /* Add task actual information */
         call missing(taskActualUser, taskActualDuration, taskStatus);
         _rc_proc_ = hParent.find();
         taskStatus=ifc(taskStatus="Started", "In-Progress", coalescec(taskStatus, "Not Started"));

         /* Add task script information */
         call missing(scriptId, scriptName);
         _rc_script_ = hScript.find();

         /* Add task analysis run information (latest analysis run for the task) */
         call missing(analysisRunStatus, analysisRunName, analysisRunUri);
         _rc_ar_ = hAR.find();
         if analysisRunStatus="RUNNING" and upcase(scriptId)=upcase("&auditScriptId.") then
            taskStatus="In-Progress";

         /* Add task attachment information (latest attachment for the task) */
         call missing(attachmentTime, attachmentName, attachmentUser, attachmentUri);
         __rc_attach__ = hAttach.find();

         /* Add task comment information (latest comment for the task) */
         call missing(commentTime, commentUser, commentText);
         __rc_comment__ = hComments.find();

      run;

   %end;


   /* Create the cycle audit tasks history table - this is the cycle's current process task history events (multiple rows for each task)
         + the following information:
      -the actual owner for any task event
      -cycle process history changes for any task event (ex: a process's variable was to set to REJECT for a task)
      -cycle attachments for the task event (all)
      -cycle comments for the task event (all)
      -cycle analysis runs for the task event (all)
   */
   %if "&outDsCycleTasksHistory." ne "" %then %do;

      data &outDsCycleTasksHistory. (keep = workflowTask auditEntry time name user status message order url);
         length workflowTask auditEntry time name user status $200 message $10000 order 8. url $2000;
         label workflowTask="Workflow Task" auditEntry="Audit Entry" time="Time" name="Name" user="User" status="Status" message="Message";
         set wf_process_tasks_hist_filtered;

         by taskCreatedTimeStamp taskOrder;

         if _N_=0 then do;
            set wf_process_tasks_hist_updated (drop=taskLatestEntryFlag taskStatus);
            set wf_process_history_w_task (drop=taskName);
            set cycle_attachements_w_task (drop=taskLatestEntryFlag);
            set cycle_comments_w_task (drop=taskLatestEntryFlag);
            set analysis_runs_w_task (drop=taskLatestEntryFlag);
         end;

         if _N_=1 then do;

            declare hash hParent(dataset: "wf_process_tasks_hist_updated");
            hParent.defineKey("taskHistoryId");
            hParent.defineData("taskActualUser");
            hParent.defineDone();

            declare hash hProcessHist(dataset: "wf_process_history_w_task", multidata: "yes");
            hProcessHist.defineKey("taskHistoryId");
            hProcessHist.defineData("processCreatedTimeStamp", "processState", "processVariableName", "processVariableValue", "processCreatedBy");
            hProcessHist.defineDone();

            declare hash hAttach(dataset: "cycle_attachements_w_task", multidata: "yes");
            hAttach.defineKey("taskHistoryId");
            hAttach.defineData("attachmentTime", "attachmentDescription", "attachmentName", "attachmentUser", "attachmentUri", "attachmentStatus", "attachmentPriorName");
            hAttach.defineDone();

            declare hash hComments(dataset: "cycle_comments_w_task", multidata: "yes");
            hComments.defineKey("taskHistoryId");
            hComments.defineData("commentTime", "commentUser", "commentText");
            hComments.defineDone();

            declare hash hAR(dataset: "analysis_runs_w_task", multidata: "yes");
            hAR.defineKey("taskHistoryId");
            hAR.defineData("analysisRunCreatedBy", "analysisRunStatus", "analysisRunName", "analysisRunStartTime", "analysisRunEndTime", "analysisRunUri");
            hAR.defineDone();

         end;

         call missing(taskActualUser);
         _rc_proc_ = hParent.find();

         /* Add the workflow task start event */
         workflowTask=taskName;
         auditEntry="Workflow Task";
         name=taskName;
         user=taskActualUser;
         status=propcase(taskState);
         time=taskCreatedTimeStamp;
         message=ifc(taskState="Started", catt("Started Workflow Task '", taskName, "'"), catt("Completed Workflow Task '", taskName, "'"));
         order=taskOrder;
         url="";
         output;

         /* Add workflow task's process history events */
         call missing(processCreatedTimeStamp, processState, processVariableName, processVariableValue, processCreatedBy);
         _rc_process_hist_ = hProcessHist.find();
         do while (_rc_process_hist_ = 0);

            workflowTask=taskName;
            auditEntry="Workflow Task";
            name=taskName;
            user=processCreatedBy;
            status=propcase(processVariableValue);
            time=processCreatedTimeStamp;
            message=catt("Set status for task '", taskName, "' to '", status, "'");
            order=taskOrder;
            url="";
            output;

            call missing(processCreatedTimeStamp, processState, processVariableName, processVariableValue, processCreatedBy);
            _rc_process_hist_ = hProcessHist.find_next();

         end;

         /* Add workflow task attachment events */
         call missing(attachmentTime, attachmentDescription, attachmentName, attachmentUser, attachmentUri, attachmentStatus, attachmentPriorName);
         __rc_attach__ = hAttach.find();
         do while (__rc_attach__ = 0);

            workflowTask=taskName;
            auditEntry="Attachment";
            name=attachmentName;
            status=attachmentStatus;
            time=attachmentTime;
            user=attachmentUser;
            message=catt(attachmentStatus, " attachment '", ifc(attachmentStatus="Replaced", attachmentPriorName, name), "'",
               ifc(attachmentStatus="Replaced", catt(" with file '", name, "'"), ""));
            order=taskOrder;
            url=attachmentUri;
            output;

            call missing(attachmentTime, attachmentDescription, attachmentName, attachmentUser, attachmentUri, attachmentStatus, attachmentPriorName);
            __rc_attach__ = hAttach.find_next();

         end;

         /* Add workflow task comment events */
         call missing(commentTime, commentUser, commentText);
         __rc_comment__ = hComments.find();
         do while (__rc_comment__ = 0);

            workflowTask=taskName;
            auditEntry="Comment";
            name="Comment";
            status="Commented";
            time=commentTime;
            user=commentUser;
            message=catt("Commented: '", commentText, "'");
            order=taskOrder;
            url="";
            output;

            call missing(commentTime, commentUser, commentText);
            __rc_comment__ = hComments.find_next();

         end;

         /* Add workflow task analysis run events */
         call missing(analysisRunCreatedBy, analysisRunStatus, analysisRunName, analysisRunStartTime, analysisRunEndTime, analysisRunUri);
         _rc_ar_ = hAR.find();
         if _rc_ar_=0 then do;

            /* Add the analysis run start event */
            workflowTask=taskName;
            auditEntry="Analysis Run";
            name=analysisRunName;
            user=analysisRunCreatedBy;
            status="Submitted";
            time=analysisRunStartTime;
            message=catt("Submitted analysis run '", analysisRunName, "'");
            order=taskOrder;
            url=analysisRunUri;
            output;

            /* Add the analysis run completed event */
            workflowTask=taskName;
            auditEntry="Analysis Run";
            name=analysisRunName;
            user=analysisRunCreatedBy;
            status=propcase(analysisRunStatus);
            time=analysisRunEndTime;
            message=catt("Completed analysis run '", analysisRunName, "' with status '", status, "'");
            order=taskOrder;
            url=analysisRunUri;
            output;

         end;

      run;

      proc sort data=&outDsCycleTasksHistory.; by time order; run;

   %end;


%mend;