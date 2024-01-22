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
                              , outDsCycleTasksSummary = audit_cycle_tasks_summary
                              , outDsCycleTasksHistory = audit_cycle_tasks_history
                              , outDsWfTemplate =
                              , outDsWfTemplateScripts =
                              , outDsWfProcessTaskHist =
                              , outDsWfProcessHist =
                              , outDsCycleAttachments =
                              , outDsCycleComments =
                              , outDsCycleAnalysisRuns =
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
         uri=catt("%sysget(SAS_SERVICES_URL)/SASRiskCirrus/Solutions/", createdInTag, "/-hash-/AnalysisRun/edit/key/", key);
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
         keep=taskId objectId name sourceSystemCd
         rename=(objectId=scriptId name=scriptName sourceSystemCd=scriptSourceSystemCd)
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
   data wf_process_tasks_hist_filtered (where=(upcase(taskName) ne "TRIGGER_UPDATE_AND_CANCEL"));
      merge wf_process_tasks_hist_filtered(firstobs=1)
            wf_process_tasks_hist_filtered(firstobs=2 keep=taskCreatedTimeStamp rename=(taskCreatedTimeStamp=taskCompletedTimeStamp));
   run;

   /* Update the workflow process tasks table (task info - entry for each task) */
   proc sort data=wf_process_tasks; by createdTimeStamp; run;
   data wf_process_tasks (where=(upcase(name) ne "TRIGGER_UPDATE_AND_CANCEL"));
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
      1. Add the active taskId and taskName to each process history event if the process event time is between the task start and task complete times
         -this is currently the best guess at the task that triggered this process event
         -do not assign the task to a process event if the process event occurred at the exact same start time as the task start
      2. Only include the process events of interest (ex: only when the CIRRUS_WORKFLOW_TRANSITIONS is changed)
   */
   proc sql;
      create table wf_process_history_w_task_init as
      select a.*, b.taskHistoryId, b.taskName, b.taskCompletedTimeStamp
      from wf_process_history as a left join wf_process_tasks_hist_filtered as b
      on b.taskCreatedTimeStamp < a.createdTimeStamp <= b.taskCompletedTimeStamp
      order by a.createdTimeStamp
      ;
   quit;

   data wf_process_history_w_task(
         keep=processId createdTimeStamp state processVariableName processVariableValue createdBy taskHistoryId taskName
         rename=(createdTimeStamp=processCreatedTimeStamp state=processState createdBy=processCreatedBy)
      );
      merge wf_process_history_w_task_init(firstobs=1)
            wf_process_history_w_task_init(firstobs=2 keep=taskHistoryId rename=(taskHistoryId=nextTaskHistoryId));
      
      retain taskVariableFlag 0 priorTaskTransition priorTaskParentId currentTaskParentId;
      
      /* If the CIRRUS_WORKFLOW_TRANSITIONS process variable was updated to any value other than CIRRUS_RUN_SCRIPT, the user must have chosen some 
      non-script execution transition.  Output those rows. */
      if state="Variable Updated" then do;
         taskVariableFlag=1;
         processVariableName=prxchange('s/^The value of variable "(\w+)".*/$1/i', -1, messageText);
         processVariableValue=prxchange('s/.*"([^"]*)"\.\s*$/$1/i', -1, messageText);
         if processVariableName="CIRRUS_WORKFLOW_TRANSITIONS" and processVariableValue ne "CIRRUS_RUN_SCRIPT" then do;
            priorTaskTransition=processVariableValue;
            priorTaskParentId=currentTaskParentId;
            output;
         end;
      end;
      
      /* last entry for this (completed) task */
      if taskHistoryId ne nextTaskHistoryId and taskCompletedTimeStamp ne "" then do;
         /* No task transitions (workflow process variable changes) were found for the completed task.  This can occur if this task's transition updated
         the same workflow variable in the same workflow process with the same value - in that case, no event is recorded in the workflow history service.  Example:
            CIRRUS_WORKFLOW_TRANSITIONS=""
            task A: sets CIRRUS_WORKFLOW_TRANSITIONS=SKIP --> recorded in workflow history service (since CIRRUS_WORKFLOW_TRANSITIONS changed from "" to "SKIP")
            task B: sets CIRRUS_WORKFLOW_TRANSITIONS=SKIP --> not recorded in workflow history service (since CIRRUS_WORKFLOW_TRANSITIONS did not change from its prior value)
         Since a task transition must have occurred, we assume that the task transition is the same as the prior task's transition.
         */
         if taskVariableFlag=0 then do;
            processVariableName="CIRRUS_WORKFLOW_TRANSITIONS";
            processVariableValue=priorTaskTransition;
            output;
         end;
         taskVariableFlag=0;
      end;
      
   run;

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
            , a.comment as attachmentComment
            , a.sourceSystemCd as attachmentSourceSystemCd
            , a.fileMimeType as attachmentFileMimeType
            , a.key as attachmentKey
            , a.fileExtension as attachmentFileExtension
            , a.grouping as attachmentGrouping
            , a.fileSize as attachmentFileSize
            , a.changeReason as attachmentChangeReason
            , a.displayName as attachmentDisplayName
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
            , a.grouping as commentGrouping
            , a.levelNum as commentLevelNum
            , a.sourceSystemCd as commentSourceSystemCd
            , a.key as commentKey
            , a.parentCommentKey as commentParentCommentKey
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
            , a.objectId as analysisRunObjectId
            , a.jobKey as analysisRunJobKey
            , a.key as analysisRunKey
            , a.sourceSystemCd as analysisRunSourceSystemCd
            , a.description as analysisRunDescription
            , a.productionFlg as analysisRunProductionFlg
            , a.createdInTag as analysisRunCreatedInTag
            , a.sharedWithTags as analysisRunSharedWithTags
            , a.scriptParameters as analysisRunScriptParameters
            , a.baseDt as analysisRunBaseDt
            , a.comments as analysisRunComments
            , a.baseDttm as analysisRunBaseDttm
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

      data &outDsCycleTasksSummary. (drop=taskHistoryId taskLatestEntryFlag _rc_: __rc_:);

         length   taskName taskLabel taskCategory taskExpectedUser taskActualUser taskStatus taskExpectedDuration taskActualDuration taskType taskId $200 taskDescription taskInfo $2000 taskLinks $32000
                  wfTemplateName wfTemplateKey $200
                  scriptId scriptName scriptSourceSystemCd $200
                  analysisRunName analysisRunStatus $200 analysisRunUri $2000 analysisRunCreatedBy analysisRunStartTime analysisRunEndTime analysisRunObjectId analysisRunJobKey analysisRunKey 
                     analysisRunSourceSystemCd analysisRunDescription analysisRunProductionFlg analysisRunCreatedInTag analysisRunSharedWithTags analysisRunBaseDt analysisRunBaseDttm $200 
                     analysisRunScriptParameters analysisRunComments $32000 
                  attachmentName attachmentUser $200 attachmentUri $2000 attachmentDescription attachmentTime $200 attachmentComment $32000 attachmentSourceSystemCd attachmentFileMimeType 
                     attachmentKey attachmentFileExtension attachmentGrouping attachmentChangeReason attachmentDisplayName $200 attachmentFileSize 8
                  commentText $32000 commentUser commentTime commentGrouping commentLevelNum commentSourceSystemCd commentKey commentParentCommentKey $200
         ;

         label taskName="Workflow Task" taskLabel="Task Label" taskCategory="Task Category" taskExpectedUser="Task Expected User" taskActualUser="Task Actual User"
                  taskStatus="Task Status" taskExpectedDuration="Task Expected Duration" taskActualDuration="Task Actual Duration" taskType="Task Type" taskDescription="Task Description"
                  taskInfo="Task Info" taskLinks="Task Links"
               wfTemplateName="Workflow Template Name" wfTemplateKey="Workflow Template Key"
               scriptId="Script Id" scriptName="Script Name" scriptSourceSystemCd="Script Source System Code"
               analysisRunName="Analysis Run Name" analysisRunStatus="Analysis Run Status" analysisRunUri="Analysis Run Uri" analysisRunCreatedBy="Analysis Run Created By"
                  analysisRunStartTime="Analysis Run Start Time" analysisRunEndTime="Analysis Run End Time" analysisRunObjectId="Analysis Run Object Id" analysisRunJobKey="Analysis Run Job Key"
                  analysisRunKey="Analysis Run Key" analysisRunSourceSystemCd="Analysis Run Source System Code" analysisRunDescription="Analysis Run Description"
                  analysisRunProductionFlg="Analysis Run Production Flag" analysisRunCreatedInTag="Analysis Run Created In Tag" analysisRunSharedWithTags="Analysis Run Shared With Tags"
                  analysisRunBaseDt="Analysis Run Base Date" analysisRunBaseDttm="Analysis Run Base Date-Time" analysisRunScriptParameters="Analysis Run Script Parameters" analysisRunComments="Analysis Run Comments"
               attachmentName="Attachment Name" attachmentUser="Attachment User" attachmentUri="Attachment Uri" attachmentDescription="Attachment Description" attachmentTime="Attachment Time"
                  attachmentComment="Attachment Comment" attachmentSourceSystemCd="Attachment Source System Code" attachmentFileMimeType="Attachment File Type" attachmentKey="Attachment Key"
                  attachmentFileExtension="Attachment File Extension" attachmentGrouping="Attachment Grouping" attachmentChangeReason="Attachment Change Reason" attachmentDisplayName="Attachment Display Name"
                  attachmentFileSize="Attachment File Size"
               commentText="Comment" commentUser="Comment User" commentTime="Comment Time" commentGrouping="Comment Grouping" commentLevelNum="Comment Level" commentSourceSystemCd="Comment Source System Code"
                  commentKey="Comment Key" commentParentCommentKey="Comment Parent Key"
         ;

         set _tmp_wftemplate_tasks_ (
               keep=wfTemplateKey id name label category duration identities desc info wfTemplateName type links
               rename=(id=taskId name=taskName label=taskLabel category=taskCategory identities=taskExpectedUser duration=taskExpectedDuration desc=taskDescription info=taskInfo type=taskType links=taskLinks)
         );

         if _N_=0 then do;
            set _tmp_wftemplate_task_scripts_;
            set wf_process_tasks_hist_updated (drop=taskProcessId);
            set cycle_attachements_w_task (drop=attachmentStatus attachmentPriorName);
            set cycle_comments_w_task;
            set analysis_runs_w_task;
         end;

         if _N_=1 then do;

            declare hash hScript(dataset: "_tmp_wftemplate_task_scripts_");
            hScript.defineKey("taskId");
            hScript.defineData("scriptId","scriptName","scriptSourceSystemCd");
            hScript.defineDone();

            declare hash hParent(dataset: "wf_process_tasks_hist_updated (where=(taskLatestEntryFlag=1))");
            hParent.defineKey("taskName");
            hParent.defineData("taskActualUser", "taskActualDuration", "taskStatus");
            hParent.defineDone();
            
            declare hash hAR(dataset: "analysis_runs_w_task (where=(taskLatestEntryFlag=1))");
            hAR.defineKey("taskName");
            hAR.defineData("analysisRunStatus", "analysisRunName", "analysisRunUri", "analysisRunCreatedBy", "analysisRunStartTime", "analysisRunEndTime", "analysisRunObjectId", "analysisRunJobKey", "analysisRunKey", "analysisRunSourceSystemCd", "analysisRunDescription", "analysisRunProductionFlg", "analysisRunCreatedInTag", "analysisRunSharedWithTags", "analysisRunScriptParameters", "analysisRunBaseDt", "analysisRunComments", "analysisRunBaseDttm");
            hAR.defineDone();

            declare hash hAttach(dataset: "cycle_attachements_w_task (where=(taskLatestEntryFlag=1))");
            hAttach.defineKey("taskName");
            hAttach.defineData("attachmentName", "attachmentUser", "attachmentUri", "attachmentDescription", "attachmentTime", "attachmentComment", "attachmentSourceSystemCd", "attachmentFileMimeType", "attachmentKey", "attachmentFileExtension", "attachmentGrouping", "attachmentFileSize", "attachmentChangeReason", "attachmentDisplayName");
            hAttach.defineDone();

            declare hash hComments(dataset: "cycle_comments_w_task (where=(taskLatestEntryFlag=1))");
            hComments.defineKey("taskName");
            hComments.defineData("commentUser", "commentText", "commentTime", "commentGrouping", "commentLevelNum", "commentSourceSystemCd", "commentKey", "commentParentCommentKey");
            hComments.defineDone();

         end;

         /* Add task actual information */
         call missing(taskActualUser, taskActualDuration, taskStatus);
         _rc_proc_ = hParent.find();
         taskStatus=ifc(taskStatus="Started", "In-Progress", coalescec(taskStatus, "Not Started"));

         /* Add task script information */
         call missing(scriptId, scriptName, scriptSourceSystemCd);
         _rc_script_ = hScript.find();

         /* Add task analysis run information (latest analysis run for the task) */
         call missing(analysisRunStatus, analysisRunName, analysisRunUri, analysisRunCreatedBy, analysisRunStartTime, analysisRunEndTime, analysisRunObjectId, analysisRunJobKey, analysisRunKey, analysisRunSourceSystemCd, analysisRunDescription, analysisRunProductionFlg, analysisRunCreatedInTag, analysisRunSharedWithTags, analysisRunScriptParameters, analysisRunBaseDt, analysisRunComments, analysisRunBaseDttm);
         _rc_ar_ = hAR.find();
         if analysisRunStatus="RUNNING" and upcase(scriptId)=upcase("&auditScriptId.") then
            taskStatus="In-Progress";

         /* Add task attachment information (latest attachment for the task) */
         call missing(attachmentName, attachmentUser, attachmentUri, attachmentDescription, attachmentTime, attachmentComment, attachmentSourceSystemCd, attachmentFileMimeType, attachmentKey, attachmentFileExtension, attachmentGrouping, attachmentFileSize, attachmentChangeReason, attachmentDisplayName);
         __rc_attach__ = hAttach.find();

         /* Add task comment information (latest comment for the task) */
         call missing(commentTime, commentUser, commentText, commentTime, commentGrouping, commentLevelNum, commentSourceSystemCd, commentKey, commentParentCommentKey);
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

      proc sort data=&outDsCycleTasksHistory.; by order time; run;

   %end;
   
   /* If any of the datasets used to create the output summary/history audit tables are requested, create them */
   %if "&outDsWfTemplate." ne "" %then %do;
      data &outDsWfTemplate.; set _tmp_wftemplate_tasks_; run;
   %end;
   %if "&outDsWfTemplateScripts." ne "" %then %do;
      data &outDsWfTemplateScripts.; set _tmp_wftemplate_task_scripts_; run;
   %end;
   %if "&outDsWfProcessTaskHist." ne "" %then %do;
      data &outDsWfProcessTaskHist.; set wf_process_tasks_hist_filtered; run;
   %end;
   %if "&outDsWfProcessHist." ne "" %then %do;
      data &outDsWfProcessHist.; set wf_process_history_w_task; run;
   %end;
   %if "&outDsCycleAttachments." ne "" %then %do;
      data &outDsCycleAttachments.; set cycle_attachements_w_task; run;
   %end;
   %if "&outDsCycleComments." ne "" %then %do;
      data &outDsCycleComments.; set cycle_comments_w_task; run;
   %end;
   %if "&outDsCycleAnalysisRuns." ne "" %then %do;
      data &outDsCycleAnalysisRuns.; set analysis_runs_w_task; run;
   %end;
   
   /* Clean up */
   %if %upcase(&debug.) ne TRUE %then %do;
      proc datasets library = work nolist nodetails nowarn;
         delete
            _tmp_analysis_run_ _tmp_analysis_run_params_
            _tmp_attachment_deleted_history_ _tmp_attachment_history_
            _tmp_cycle_ar_link_insts_ _tmp_cycle_attachments_ _tmp_cycle_comments_ _tmp_cycle_summary_ _tmp_cycle_wftemp_link_insts_
            _tmp_wftemplate_summary_ _tmp_wftemplate_task_scripts_ _tmp_wftemplate_tasks_
            analysis_run_params analysis_runs analysis_runs_w_task
            attachment_history_all
            cycle_attachements_w_task cycle_comments_w_task
            wf_process wf_process_history wf_process_history_w_task wf_process_history_w_task_init 
            wf_process_tasks wf_process_tasks_hist_filtered wf_process_tasks_hist_updated wf_process_tasks_history
            ;
      quit;
   %end;


%mend;