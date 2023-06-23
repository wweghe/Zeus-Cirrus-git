--[[
/* Copyright (C) 2022-2023 SAS Institute Inc. Cary, NC, USA */

/*!
\file    lock_manager.lua

\brief   This module is about lock management

\ingroup commonAnalytics

\author  SAS Institute Inc.

\date    2015

\IMPORTANT NOTES:
    ALL locking activity (including logging to the locking activity log) must be done
    within this module **with the critical code lock held**.
    The critical code lock **must be released** prior to exiting code in this module (or while
    sleeping in this module).

    The API for the lock manager is coded to not throw exceptions when locks are not obtained/freed -- but only
    thrown when programming errors are found.
    This was done because if an exception is received at the sas_macro_entrypoint, it is interpreted as a
    Lua error and the RC set for RSK_CALL_RISKEXEC is set to non-zero (interpreted as an error) and then
    the process is terminated.

    Please see individual function prologue comments for background and details.
    Only functions that are visible outside this module (by doing a M.x = x after the function definition)
    have good prologue comments
*/

]]


local sasdata = require 'sas.risk.utils.sasdata'
local errors = require 'sas.risk.utils.errors'
local stringutils = require 'sas.risk.utils.stringutils'
local tableutils = require 'sas.risk.utils.tableutils'
local filesys = require 'sas.risk.utils.filesys'

local M = {}


local do_lock = function(lock_ds)
   return sas.lock_ds(lock_ds)
end
-- do not export this function outside this module


local do_unlock = function(lock_ds)
   return sas.unlock_ds(lock_ds)
end
-- do not export this function outside this module


--- Logs locking activity to a dataset that will help diagnose any locking problems
--
--  Note: callers must hold the critical code lock!!
local _log_activity = function(locks_libref, action, lock_id, other)
   other = tostring(other or '')
   sasdata.sql[[insert into @locks_libref@.locking_activity
                   set timestamp=datetime(),
                       action="@action@",
                       lock_id="@lock_id@",
                       other="@other@"]]
end

local log_activity = function(locks_libref, action, lock_id, other)

   -- Allow for problems logging our locking activity without blowing out (and potentially continuing to hold the locking critical code lock)
   -- which could cause all locking to stop for the user or system.
   local success, results = errors.protected_call(_log_activity, locks_libref, action, lock_id, other)
   if not success then
      sas.print("%2ZCould not log locking activity.")
   end
end
-- do not export this function outside this module


---The activity log for the locks_libref will always contain data less than this many days old, and never more than 2* this number of days old.
local prune_days=2

--- Prunes the locking activity log to keep it from growing endlessly - but retains recent locking activity in order to
--  debug locking problems.
--  All prunings are done every "prune_days".
--  The 1st pruning is done after "prune_days" with very few log entries pruned (because we want to keep "prune_days" worth of log entries in the log).
--  The 2nd and subsequent prunings are done pruning a full "prune_days" worth of log entries.
--
--  Note: callers must hold the critical code lock!!
local prune_activity_log = function(locks_libref, last_prune_activity_log_dttm)
   -- if it's been more than prune_days since last pruning, then prune
   local prune_point = sas.datetime() - 3600*24*prune_days

   local pruning_is_due = prune_point > last_prune_activity_log_dttm
   if pruning_is_due  then
      -- prune activity older than prune_days old.
      sasdata.sql([[delete from @locks_libref@.locking_activity
                  where timestamp < @prune_point@;]], {locks_libref=locks_libref, prune_point=prune_point})

      -- update the last pruned dttm
      sas.submit([[
         data @locks_libref@.locking_code_semaphore;
            set @locks_libref@.locking_code_semaphore;
            last_prune_activity_log_dttm = datetime();
         run;
         ]], {locks_libref=locks_libref})

      log_activity(locks_libref, "PRUN", sas.putn(prune_point,"NLDATMS."), sas.symget("sysjobid"))
   end
end
-- do not export this function outside this module


---
-- acquire_critical_code_lock - Locks the section of locking critical code so that only one user (even across processes)
-- can execute it at one time.
--
-- Returns a boolean indication of whether the critical code lock could be acquired in the time given
--
local acquire_critical_code_lock = function(locks_libref, max_time_to_wait)

   max_time_to_wait = max_time_to_wait or -1   -- -1 means forever

   local sleep_time_millis=200
   local waited_time=0

   --
   -- Try to acquire an exclusive lock on the critical code so we can perform locking activities.
   -- Keep trying to get the critical code lock while we do not get it and we've not waited too long
   while do_lock(locks_libref .. '.' .. "locking_code_semaphore") ~= 0 do

      sas.print("%3ZCould not acquire critical code lock, will try again.  If this condition persists, check filesystem permissions for write capability to " .. sas.pathname(locks_libref))

      -- We did not get the critical code lock.
      -- Sleep and try to get it again.
      sas.sleep_millis(sleep_time_millis)

      -- Calculate how much we've slept and whether we've hit the max wait time
      if (max_time_to_wait ~= -1)  then
         waited_time=waited_time + sleep_time_millis
         if waited_time > max_time_to_wait*1000 then
            -- if could not get the critical code lock in the allotted time, tell the caller that we've failed.
            -- We cannot log to the locking_activity log because we do not hold the critical code lock at this point
            sas.print("%1ZCould not get critical code lock in allotted time.")
            return false
         end
      end
   end

   -- acquired critical code lock (this log point must be after actual cc lock is acquired)
   log_activity(locks_libref, "a", "cc", sas.symget("sysjobid"))  -- acquired critical code lock
   return true
end
-- do not export this function, it should only be used by this module



---
-- release_critical_code_lock - Unlocks the section of locking critical code so that only one user can execute it at one time.
--
local release_critical_code_lock = function(locks_libref)
   log_activity(locks_libref, "r", "cc", sas.symget("sysjobid")) -- released critical code lock (must be BEFORE actual unlock)
   if do_unlock(locks_libref .. '.' .. "locking_code_semaphore") ~= 0 then
      sas.print("%1ZFailed to release the locking critical code lock!  Possible locking problem exists.")
      errors.throw("failed_to_release_critical_code_lock")
   end
end
-- do not export this function, it should only be used by this module


-- forward declaration
local new_lock
local process_potential_blocking_lock
local get_potential_blocking_locks

---
-- lock_object  - Lock an object using the cooperative/advisory locking paradigm
--

--[=[
  Purpose:  Locks a given "object name" from other users who use lock_object() and unlock_object().
            The lock obtained is a cooperative lock (also called an advisory lock) in that all users
            who reference the true objects must protect their access to them by using these
            locking functions on their names.

  References:
               http://en.wikipedia.org/wiki/Advisory_lock
               http://en.wikipedia.org/wiki/Deadlock

  Terminology:
                "system-level lock"  -- a lock on an object located at the system/product level within the product (e.g. a lock on the SDM)
                "user-level lock"    -- a lock on an object located at the user level within the product (e.g. PSDM)
                "critical code lock"  -- the lock that guards our locking logic when operating on a lock directory/libref (one lock dir per user and one at the product level).
                                         A programmer has no direct control over the critical code lock -- it is used by the locking functions themselves for implementing the locking algorithms.
                                         This lock is also called the "locking code semaphore".


  NOTES:  This function is designed to fail (not attempt to acquire a lock) when the SAS system goes into syntax checking mode.

  Implementation notes:
  - lock_object and unlock_object implement "cooperative" a.k.a. "advisory" locking.  This type of locking
    is where all access to protected data (protected by a lock) must be accessed after locking it appropriately (via these functions).
    Unless the system is in a known, dormant state, no access to that data must be performed without getting the
    appropriate lock otherwise unpredictable results can occur (deadlock, etc.)

  - Our means of an atomic get/set lock is provided by accessing SAS' lock function via sas.lock_ds on the "critical code lock".
    This lock protects/guards all locking logic -- requiring it to be executed in at most 1 SAS process
    at one time.
  - Our means of automatically dropping locks when a SAS session ends:  LOCKs on SAS data sets perform this for us.
    For each user-requested lock, we create and lock one SAS dataset.  So if 5 SAS sessions request a Read-lock on
    the SDM, that is 5 read-locks on 5 created data sets ... each LOCKed by their respective SAS session.
  - Our means for testing if a lock is being held:
    Go through all of the potential lock data sets based on the object name and lock type requested.  Some locks may exist, some may not
    because a process may have ended and left the data set around.  For each potential lock data set, try locking it.
        - If you succeed in locking it, unlock it and delete it.  The user of the lock is no longer around.
        - If you don't succeed in locking it, someone still has the lock to it (and it may be your session).
          This is a blocking lock to the lock the user requested.  Stay in the wait loop trying again.
  - We also note that when an STP fails, it nearly always completes and returns, rarely spinning forever in shipped code ...
    though easily done in untested code.
  - Because these are real locks, if a SAS session holds a lock, that SAS session must release the lock or
    the session must be killed in order for the lock to be dropped.

  - The locking that these locking functions provides is "cooperative locking".  That is, no user SAS data sets or OS-level locking
    occurs, only the *object name* is locked as requested by the caller.  If other users
    want access to that object name, they must use these functions otherwise the locking will be circumvented and be useless.
    **That means** not only must users who WRITE to the named object go through these functions, users who READ from them
    must also get a lock on the object.

  NOTES:
  - A slightly fairer, queue-based algorithm would be better than a semi-random one based on when a process wakes ... a potential future enhancement.

  PARAMETERS:
    locks_libref = the libref to a lock directory that has been set up using ensure_lock_directory

    obj= name of the object (case insensitive).  Object names are limited to 100 chars.

    lock_type= R or W  (read or write)
    max_time_to_wait=-1  means wait forever
                         otherwise the number of seconds to wait before giving up trying to get the user lock.

   RETURNS:
      the unique lock ID that must be used when unlocking the object or
      nil when the lock cannot be acquired in the time given

--]=]
local lock_object = function(locks_libref, obj, lock_type, max_time_to_wait)

   max_time_to_wait = tonumber(max_time_to_wait) or -1 -- wait forever if not specified

   -- don't even try to perform locking if OBS is set to 0, though we try to allow unlocking
   local obs = sas.getoption("OBS")
   if obs == 0 then
      errors.throw("utils_option_obs_0_when_locking")
   end

   obj = string.upper(obj)                  -- all obj names are case-insensitive
   lock_type= string.upper(lock_type)


   if obj == "" then
      sas.print ("%1Z".. "Coding error: lock_manager.lock_object() - missing OBJ argument.")
      errors.throw("coding_error")
   end
   if string.len(obj) > 100 then
      sas.print ("%1Z".. "Coding error: lock_manager.lock_object() - OBJ value is too long:" .. obj)
      errors.throw("coding_error")
   end
   if lock_type ~= "R"  and lock_type ~= "W" then
      sas.print ("%1Z".. "Coding error: lock_manager.lock_object() - missing/invalid LOCK_TYPE argument")
      errors.throw("coding_error")
   end

   sasdata.require_libref(locks_libref)
   sasdata.require_ds(locks_libref .. '.' .. "locking_code_semaphore")

   -- don't polute the SAS log w/ statements from the locking code details if possible
   sas.set_quiet(true)
   local saved_notes = sas.getoption("NOTES")
   -- Debug("saved notes is " .. tostring(saved_notes))
   if saved_notes then saved_notes="NOTES" else saved_notes="NONOTES" end
   -- sas.setoption("NOTES", false)   -- this wasn't always successful, so we are submitting code
   sas.submit[[options nonotes;]]
   -- Debug("option now is " .. tostring(sas.getoption("NOTES")))

   local sleep_time_millis=1000
   local waited_time=0.0
   local wait_over = false

   local new_lock_id_prefix
   local lock_tables
   local blocking_lock_info

   while not wait_over do

      if not acquire_critical_code_lock(locks_libref, max_time_to_wait) then
         --sas.setoption("notes", saved_notes)   -- this wasn't always successful, so we are submitting code
         sas.submit([[options @saved_notes@;]], {saved_notes=saved_notes})
         sas.set_quiet(false)
         return nil
      end

      -- We don't accumulate the time waiting for the critical code lock into the time spent for waiting for the user lock ... this waiting time
      -- is usually very short

      -- VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
      --               This code now has the exclusive critical code lock
      --               that must be released before we return
      -- VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV

       --
       -- See if any blocking locks exist that block this user's ability to get the requested lock.
       --
      lock_tables, new_lock_id_prefix = get_potential_blocking_locks(locks_libref, obj, lock_type)

     -- Debug("Potential blocking locks:" .. tableutils.to_string(lock_tables))

      -------------------------------------------------------------------------------------------------
      --    Determine if a blocking lock truly exists on the object.
      --    A SAS session may have ended, releasing the lock on the data set without having gone through unlock_object().
      -------------------------------------------------------------------------------------------------
      local table_count=#lock_tables
      local i=0
      blocking_lock_info=nil

      while (i < table_count) and (not blocking_lock_info) do
         i=i+1
         blocking_lock_info = process_potential_blocking_lock(locks_libref, lock_tables[i])

         if (blocking_lock_info) then
             -- Log something noting that the user requested lock is blocked by another lock
             log_activity(locks_libref, lock_type .. "BLK", lock_tables[i], tostring(blocking_lock_info.process_id))

             -- We did not get the lock because a blocking lock exists.
             -- Release the critical code lock and get out of this inner loop and start over (after sleeping)

             -- We *must* free the critical code lock for others to do lock work ... such as unlocking the blocking lock we are waiting on.
             -- We will try to obtain the critical code lock again at the top of the outer loop.
            release_critical_code_lock(locks_libref)

             -- Sleep and try to get it again the next time through the outer loop.
            sas.sleep_millis(sleep_time_millis)

            -- Calculate how much we've slept and whether we've hit the max wait time
            if max_time_to_wait ~= -1 then
               waited_time = waited_time + sleep_time_millis
               if waited_time > max_time_to_wait*1000 then
                  wait_over = true
               end
            end
          end
      end  -- loop through potential blocking locks

      if not blocking_lock_info then
         --  hooray, no blocking operations are currently happening on this object, so we can get the type of lock we want on this object
         local the_lock = new_lock(locks_libref, new_lock_id_prefix, obj, lock_type)
         --sas.setoption("notes", saved_notes)   -- this wasn't always successful, so we are submitting code
         sas.submit([[options @saved_notes@;]], {saved_notes=saved_notes})
         sas.set_quiet(false)
         return the_lock
      end
   end  -- end outer while loop trying to get the user lock

   -- we've waited too long ... log a timeout event and return nil indicating we could not get the lock
   log_activity(locks_libref, "TIMO", new_lock_id_prefix, sas.symget("sysjobid")) -- timeout getting lock
   blocking_lock_info = blocking_lock_info or {}
   sas.print("%3ZCould not get " .. lock_type .. " lock on " .. obj .. " within " .. tostring(max_time_to_wait) .. " seconds.  User " .. tostring(blocking_lock_info.user_name) .. " (process id: " .. tostring(blocking_lock_info.process_id) .. ") holds a " .. tostring(blocking_lock_info.lock_type) .. " lock on " .. tostring(obj) .. " that was acquired at " .. tostring(sas.putn(blocking_lock_info.lock_acquired_dttm,"NLDATMS.")));
   --sas.setoption("notes", saved_notes)   -- this wasn't always successful, so we are submitting code
   sas.submit([[options @saved_notes@;]], {saved_notes=saved_notes})
   sas.set_quiet(false)
   return nil
end
M.lock_object = lock_object  -- let this function be seen from outside this module


get_potential_blocking_locks = function(locks_libref, obj, lock_type)
      -- Get the unique number (within the lock directory) for this object name
      local obj_number = sasdata.select_one_value([[
          select object_number
          from @locks_libref@.locks_obj_names_to_numbers
          where object_name eq "@obj@"]],{locks_libref=locks_libref, obj=obj})

      if obj_number == nil then
        obj_number=sasdata.get_row_count(locks_libref .. "." .. "locks_obj_names_to_numbers")+1

        sasdata.sql[[insert into @locks_libref@.locks_obj_names_to_numbers
                        set object_name="@obj@",
                            object_number=@obj_number@]]
      end

      -- Create the 1st part of the data set name, we append to it later.
      local new_lock_id_prefix = "LOCK_" .. tostring(obj_number) .. '_' .. lock_type .. "_"

      -- See if there is a blocking lock on the object the user wants a lock on
      -- Any lock type blocks a requested W lock
      -- A W lock type blocks a requested R lock
      local blocking_lock_type_suffix="" --  search for both R and W locks when user wants a W lock
      if lock_type == "R" then
         blocking_lock_type_suffix="W"  -- See if any W locks exist when user wants a R lock
      end

     -- See if there is a blocking lock on the object the user wants a lock on
     -- Find all blocking locks for the given lock object.
     -- First query the set of locks on the given object ... but only those lock types that
     -- would block this lock request.
     local lock_tables = sasdata.select_one_column([[
            select ds_name
            from @locks_libref@.locks
            where ds_name like "LOCK^_@obj_number@^_@suffix@%" escape '^']],
            {locks_libref=locks_libref, obj_number=obj_number, suffix=blocking_lock_type_suffix})
     return lock_tables, new_lock_id_prefix
end
-- do not export this function, it should only be used by this module


--[=[
       For a potential blocking lock, try to lock the data set.
       - if you succeed in locking it, unlock it and delete it.  The user of the lock (session who created the lock) is no longer around.
       - if you don't succeed in locking it, someone still has the lock to it.
         This is a blocking lock to the lock the user requested.
--]=]
process_potential_blocking_lock = function(locks_libref, lock_table)
   local blocking_lock_info = nil

   local lock_ds = locks_libref .. '.' .. lock_table
    -- See if the lock table exists first.  We can do this check (and succeed in testing it) even if the table is locked.
    if not sasdata.ds_exists(lock_ds) then
      -- The lock table no longer exists, so its lock does not exist.  Let's clean up our bookkeeping table.
      sasdata.sql([[delete from @locks_libref@.locks
                  where ds_name eq "@lock_table@";]], {locks_libref=locks_libref, lock_table=lock_table})

    -- Table exists, try to lock the lock table.
    elseif do_lock(lock_ds) == 0 then

        -- succeeded in locking it -- the session that locked it is no longer around or doesn't need this lock dataset, delete it
         sasdata.delete_ds(lock_ds)
         sasdata.sql([[delete from @locks_libref@.locks
                       where ds_name eq "@lock_table@";]], {locks_libref=locks_libref, lock_table=lock_table})
    else
       -- Failed to lock a lock table.  ** A user has a blocking lock on the lock we want** (for example, we want to write to something someone else is
       -- reading or writing ... or we want to read something someone else is writing).  We must wait and try to get the lock again.

       -- Get the blocking lock holder info to return
       -- When multiple blocking locks exist, this info is for the 1st one we come across

       blocking_lock_info = sasdata.select_one_row(
             [[select lock_acquired_dttm,
                     user_name,
                     lock_type,
                     process_id
               from @locks_libref@.locks
               where ds_name eq "@lock_table@"]],
               {locks_libref=locks_libref, lock_table=lock_table})
    end

   return blocking_lock_info
end
-- do not export this function, it should only be used by this module



-- (Forward declared) returns a new lock id
new_lock = function(locks_libref, new_lock_id_prefix, obj, lock_type)
     --[=[
        Data sets created for each lock are named:  lock_<object number>_<lock_type>_<lock_number>
        where:
           <object_number> is a unique number within the lock directory for the object name given.
                           This number is used in the data set name to allow for longer names than would fit
                           if we would have use the name itself in the data set name.
           <lock_type>     R or W
           <lock_number>   Every new lock within the lock directory is given a unique number, each increasing by 1
     --]=]


     -- Get next lock number and bump it.
     -- This code can't use a regular (non-modify) data step because it replaces the data set
     -- which is our locking code semaphore ... and drops the SAS lock on it if we did so.
     -- We also can't use proc sql/update because it currently drops the lock.
     -- So we have to use data step to modify the existing data set.
     -- now get the next lock number and increment it by 1
     sas.submit[[
         data @locks_libref@.locking_code_semaphore;
            modify @locks_libref@.locking_code_semaphore;   /* modify the existing data set in place */
            call symput("next_lock_num", next_lock_num);
            call symput("last_prune_activity_log_dttm", last_prune_activity_log_dttm);
            next_lock_num=next_lock_num+1;  /* bump it */
         run;]]

     local new_lock_id = new_lock_id_prefix .. tostring(sas.symget("next_lock_num"))
     local last_prune_activity_log_dttm = sas.symget("last_prune_activity_log_dttm")


    -- Create the locking data set for this lock.
    -- Storing info in the data set does not give value because you can't look at it
    -- when it is locked.  So we create a tiny data set and store more useful info
    -- in the locks table
    local lock_ds = locks_libref .. '.' .. new_lock_id
    sas.submit[[
         data @lock_ds@;
            _nothing_ = .;
         run;
     ]]

    -- Now lock the new data set.
    -- This indicates that this SAS session has the particular type of lock on that object.
    -- The lock will go away when this session's code unlocks it or when this sas session goes away.
     do_lock(lock_ds)

     log_activity(locks_libref, "ACQD", new_lock_id, sas.symget("sysjobid"))

     sasdata.sql[[
        insert into @locks_libref@.locks
                    set object_name="@obj@",
                        lock_type="@lock_type@",
                        ds_name="@new_lock_id@",
                        lock_acquired_dttm=%sysfunc(datetime()),
                        user_name="&rsk_user",
                        process_id="&sysjobid"]]

     -- before we release the CC lock, prune the activity log if it's time
     prune_activity_log(locks_libref, last_prune_activity_log_dttm)


     -- release the critical code lock, so other locking activity can occur
     release_critical_code_lock(locks_libref)

   --  We have successfully acquired the lock the user requested
     sas.print("%3ZLock of " .. obj .. " successful, lock ID is " .. new_lock_id)
     return new_lock_id  -- the output lock id is the dataset name (unbeknownst to the caller)
end
-- do not export this function, it should only be used by this module



--- unlock_object  - unlocks an object using the lock_id acquired when locking it

--[=[
  Purpose:  Unlocks a given object from other users who use lock_object/unlock_object.
            The lock ID given must have been obtained from lock_object()

            See more detailed comments in lock_object

  References:
               http://en.wikipedia.org/wiki/Advisory_lock
               http://en.wikipedia.org/wiki/Deadlock

  NOTES:  This function will succeed in releasing an existing lock when the SAS system has the OBS system option set to 0
          (as is done when SAS goes into syntax checking mode),
          though the message put out will indicate that the name of the object being unlocked is unknown.

  Returns true if the given lock ID was unlocked, false otherwise
--]=]
local unlock_object = function(locks_libref, lock_id)

   sasdata.require_libref(locks_libref)
   sasdata.require_ds(locks_libref .. '.' .. "locking_code_semaphore")


   if string.len(lock_id) > 5 then
      if string.sub(lock_id, 1,5) ~= "LOCK_" then --  only allow locks, none of our restricted tables that implement locking
         errors.throw("invalid id to unlock: " .. lock_id)
      end
   else
      errors.throw("invalid id to unlock: " .. lock_id)
   end


   -- don't polute the SAS log w/ statements from the locking code
   sas.set_quiet(true)
   local saved_notes = sas.getoption("NOTES")
   -- Debug("saved notes is " .. tostring(saved_notes))
   if saved_notes then saved_notes="NOTES" else saved_notes="NONOTES" end
   -- sas.setoption("NOTES", false)   -- this wasn't always successful, so we are submitting code
   sas.submit[[options nonotes;]]
   -- Debug("option now is " .. tostring(sas.getoption("NOTES")))


   local out_rc = false
   if not acquire_critical_code_lock(locks_libref) then
      --sas.setoption("notes", saved_notes)   -- this wasn't always successful, so we are submitting code
      sas.submit([[options @saved_notes@;]], {saved_notes=saved_notes})
      sas.set_quiet(false)
      return out_rc
   end


   ----VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
   --      This code now has an exclusive lock on the critical code lock  that must be released before we return
   ----VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV

   lock_id=string.upper(lock_id)  -- this is essentially a dataset name, so make it case insensitive so that our queries work well

   local lock_ds = locks_libref .. '.' .. lock_id
   if sasdata.ds_exists(lock_ds) then
      -- clear the user lock
      local unlock_rc = do_unlock(lock_ds)
      if unlock_rc ~= 0 then
         log_activity(locks_libref, "RLSX", lock_id, "rc" .. tostring(unlock_rc))

        -- Could not release the lock.  It's possible that the user unlocked his lock multiple times,
        -- used the wrong unlock libref (system vs. user), or gave the wrong lock id (that of another session), or ?
         sas.print("%1ZCould not release lock " .. lock_id);
      else
         log_activity(locks_libref, "RLSD", lock_id, sas.symget("sysjobid"))

       -- We cleared the lock successfully, delete the bookkeeping for the lock.
       -- Note that none of this bookkeeping will get cleaned up if the SAS Session running this
       -- function is in syntax checking mode.  That's not too bad, since the real lock got released/cleared.
       -- Our locking logic will cause the lock data set and its entry in the LOCKS table to be deleted eventually
       -- when it if found to not be locked by anyone.
         sasdata.delete_ds(lock_ds)

         -- Don't worry if we can't delete it ... any failure does not hurt the integrity of future locking.

         local object_name =  sasdata.select_one_value([[
            select object_name   /* get the object name to put in the output msg */
             from @locks_libref@.locks
             where ds_name eq "@lock_id@";
              ]],  {locks_libref=locks_libref, lock_id=lock_id})
         sasdata.sql([[
            delete from @locks_libref@.locks
                 where ds_name eq "@lock_id@";
            ]], {locks_libref=locks_libref, lock_id=lock_id})

         if not object_name then
            object_name = "unknown"  -- when OBS has been set to 0 ... unlocking is successful, but we can't see what the object name was
         end

         -- We leave the name to number mapping in the table  locks_obj_names_to_numbers ... we use it again

         sas.print("%3ZUnlock of " .. object_name .. " successful, lock ID was " .. lock_id);
         out_rc=true
      end
   else
      log_activity(locks_libref, "NOLK", lock_id, sas.symget("sysjobid"))
      sas.print("%1ZLock " .. lock_id .. " does not exist and therefore cannot be unlocked")
   end


   -- release the critical code lock, so other locking activity can occur
   release_critical_code_lock(locks_libref)

   --sas.setoption("notes", saved_notes)   -- this wasn't always successful, so we are submitting code
   sas.submit([[options @saved_notes@;]], {saved_notes=saved_notes})
   sas.set_quiet(false)
   return out_rc
end
M.unlock_object = unlock_object   -- let this function be seen from outside this module


--- This function does a SYSTEM-wide protected call surrounded by a lock.  To do a user-scoped protected call surrounded by a lock,
--  use the similar function in user.lua
local protected_call_with_lock = function(locks_libref, object_name, lock_type, ftn, ...)

   -- Surround the protected call to ftn with a lock of the specified name & type scoped to the given locks_libref
   local lock_id = lock_object(locks_libref, object_name, lock_type, -1)
   if lock_id == nil then
      errors.throw("Could not get lock for " .. object_name .. ".  See SAS log for details.");
   end

   -- Make the call we were asked to make
   local ok, r1, r2, r3, r4, r5 = errors.protected_call(ftn, ...)

   -- Release the lock immediately after the call
   local unlocked = unlock_object(locks_libref, lock_id)
   if not unlocked then
      errors.throw("Could not release lock_id " .. lock_id .. ".  See SAS log for details.");
   end

   if not ok then -- if an exception occurred calling the ftn, rethrow it
      error(r1)  -- rethrow the error AFTER releasing the lock, giving the error table from errors.protected_call()
   end

   return r1, r2, r3, r4, r5   -- return any results from the ftn called
end
M.protected_call_with_lock = protected_call_with_lock



--- Verify that locking works with the specified locking directory (libref).
--  Sometimes when file permissions (e.g. ownership) are not set up appropriately, it is possible that locking will fail miserably.
--  This is a quick test to verify that locking with the specified locking library works.
local verify_locking = function(locks_libref)

   -- There should be a better & quicker way to verify this, but for now this is what we have.

   -- Wait up to an extremely reasonable amount of time (in seconds) to acquire the
   -- critical code lock, if can't get it in this time, blow out with a meaninful error message.
   -- 30 seconds is too low and this fails when the system is heavily loaded (as seen in our nightly tests).
   if not acquire_critical_code_lock(locks_libref, 300) then
      errors.throw("Locking verification failed.  Be sure appropriate ownership and permissions are set on this product's 'data' and 'indata' directories.");
   end
   release_critical_code_lock(locks_libref)
end


---
-- Assigns the given libref to the given directory to be used for locking purposes
-- and prepares/ensures that data sets necessary for locking.
--
local ensure_lock_directory = function(libref, path)
   filesys.mkdir(path)
   sasdata.libname(libref, path)

   -- Holds info about each successful lock acquired by the lock_object caller.
   -- The dataset doesn't necessarily contain an accurate set of locks at any one time
   -- because processes may have been cancelled or stopped or failed.  When this happens,
   -- the locks on the SAS datasets are automatically dropped but this table doesn't get updated
   -- and can become a bit stale.  That's OK.  This info is only used for putting out messages
   -- as to who is holding locks and for debugging purposes.
   local ds=libref .. '.LOCKS'
   if not sasdata.ds_exists(ds) then
      sas.submit[[data @ds@ (reuse=yes);  /* reuse records deleted when locks are released */
                  length object_name $100 lock_type $1 ds_name $32 lock_acquired_dttm 8 user_name $64 process_id $32;
                  format lock_acquired_dttm NLDATMS.;
                  stop;
                  run;]]
   end


   -- The following data set has two purposes.
   -- First, this data set holds the "next lock number" to be used when creating the lock data set names.
   -- But more importantly, this data set is used as a semaphore to serialize across all processes the running of the locking code (a.k.a. "locking critical code").
   ds = libref .. ".LOCKING_CODE_SEMAPHORE"
   if not sasdata.ds_exists(ds) then
      sas.submit[[data @ds@;
                  format last_prune_activity_log_dttm  NLDATMS.;
                  next_lock_num=1;
                  last_prune_activity_log_dttm=datetime();
                  run;]]
   end

   -- Each locked "name" is mapped to a number so that the number takes a small number of characters in the
   -- eventual locked data set name
   ds = libref .. ".LOCKS_OBJ_NAMES_TO_NUMBERS"
   if not sasdata.ds_exists(ds) then
      sas.submit[[data @ds@;
                  length object_name $100 object_number 8;
                  stop;
                  run;]]
   end


   -- The locking activity log contains data to help debug locking problems like deadlock, etc.
   ds = libref .. ".LOCKING_ACTIVITY"
   if not sasdata.ds_exists(ds) then
      sas.submit[[data @ds@ (reuse=yes);  /* reuse records deleted when we auto-prune the log */
                  length timestamp 8 action $4 lock_id $20 other $10;
                  format timestamp NLDATMS.;
                  stop;
                  run;]]
   end

   verify_locking(libref)
end
M.ensure_lock_directory = ensure_lock_directory





return M
-- always have a line after the final loc (the Lua 5.1 parser ignores the last line for some reason)
