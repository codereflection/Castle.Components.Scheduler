// Copyright 2004-2009 Castle Project - http://www.castleproject.org/
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Generic;
using System.Linq;

namespace Castle.Components.Scheduler
{
	using System;
	using System.Globalization;
	using Utilities;

	/// <summary>
	/// Encapsulates an algorithm for generating regular periodic triggers
	/// relative to some fixed start time.  The trigger will fire repeatedly
	/// every recurrence period until either the remainind number of recurrences
	/// drops to zero, the end time is reached, or the associated job is deleted.
	/// </summary>
	[Serializable]
	public class PeriodicTrigger : Trigger
	{
		/// <summary>
		/// The default misfire action.
		/// </summary>
		public const TriggerScheduleAction DefaultMisfireAction = TriggerScheduleAction.Skip;

	    internal DateTime startTimeUtc;
	    internal DateTime? endTimeUtc;
	    internal TimeSpan? period;
	    internal int? jobExecutionCountRemaining;
	    internal bool isFirstTime;

	    internal TimeSpan? misfireThreshold;
	    internal TriggerScheduleAction misfireAction;

	    internal DateTime? nextFireTimeUtc;

		/// <summary>
		/// Creates a periodic trigger.
		/// </summary>
		/// <param name="startTimeUtc">The UTC date and time when the trigger will first fire</param>
		/// <param name="endTimeUtc">The UTC date and time when the trigger must stop firing.
		/// If the time is set to null, the trigger may continue firing indefinitely.</param>
		/// <param name="period">The recurrence period of the trigger.
		/// If the period is set to null, the trigger will fire exactly once
		/// and never recur.</param>
		/// <param name="jobExecutionCount">The number of job executions remaining before the trigger
		/// stops firing.  This number is decremented each time the job executes
		/// until it reaches zero.  If the count is set to null, the number of times the job
		/// may execute is unlimited.</param>
		/// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="period"/> is negative or zero</exception>
		/// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="jobExecutionCount"/> is negative</exception>
		public PeriodicTrigger(DateTime startTimeUtc, DateTime? endTimeUtc, TimeSpan? period, int? jobExecutionCount)
		{
			if (period.HasValue && period.Value.Ticks <= 0)
				throw new ArgumentOutOfRangeException("period", "The recurrence period must not be negative or zero.");
			if (jobExecutionCount.HasValue && jobExecutionCount.Value < 0)
				throw new ArgumentOutOfRangeException("jobExecutionCount", "The job execution count remaining must not be negative.");

			this.startTimeUtc = DateTimeUtils.AssumeUniversalTime(startTimeUtc);
			this.endTimeUtc = DateTimeUtils.AssumeUniversalTime(endTimeUtc);
			this.period = period;
			jobExecutionCountRemaining = jobExecutionCount;
			isFirstTime = true;

			misfireAction = DefaultMisfireAction;
		}

		/// <summary>
		/// Creates a trigger that fires exactly once at the specified time.
		/// </summary>
		/// <param name="fireTimeUtc">The UTC time at which the trigger should fire</param>
		/// <returns>The one-shot trigger</returns>
		public static PeriodicTrigger CreateOneShotTrigger(DateTime fireTimeUtc)
		{
			return new PeriodicTrigger(fireTimeUtc, null, null, 1);
		}


		/// <summary>
		/// Creates a trigger that fires every 24 hours beginning at the specified start time.
		/// </summary>
		/// <remarks>
		/// This method does not take into account local time variations such as Daylight
		/// Saving Time.  Use a more sophisticated calendar-based trigger for that purpose.
		/// </remarks>
		/// <param name="startTimeUtc">The UTC date and time when the trigger will first fire</param>
		public static PeriodicTrigger CreateDailyTrigger(DateTime startTimeUtc)
		{
			return new PeriodicTrigger(startTimeUtc, null, new TimeSpan(24, 0, 0), null);
		}

		/// <summary>
		/// Gets or sets the UTC date and time when the trigger will first fire.
		/// </summary>
		public DateTime StartTimeUtc
		{
			get { return startTimeUtc; }
			set { startTimeUtc = DateTimeUtils.AssumeUniversalTime(value); }
		}

		/// <summary>
		/// Gets or sets the UTC date and time when the trigger must stop firing.
		/// If the time is set to null, the trigger may continue firing indefinitely.
		/// </summary>
		public DateTime? EndTimeUtc
		{
			get { return endTimeUtc; }
			set { endTimeUtc = DateTimeUtils.AssumeUniversalTime(value); }
		}

		/// <summary>
		/// Gets or sets the recurrence period of the trigger.
		/// If the period is set to null, the trigger will fire exactly once
		/// and never recur.
		/// </summary>
		/// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="value"/> is negative or zero</exception>
		public TimeSpan? Period
		{
			get { return period; }
			set
			{
				if (value.HasValue && value.Value.Ticks <= 0)
					throw new ArgumentOutOfRangeException("value", "The recurrence period must not be negative or zero.");

				period = value;
			}
		}


		/// <summary>
		/// Gets or sets the number of job executions remaining before the trigger
		/// stops firing.  This number is decremented each time the job executes
		/// until it reaches zero.  If the count is set to null, the number of times
		/// the job may execute is unlimited.
		/// </summary>
		/// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="value"/> is negative</exception>
		public int? JobExecutionCountRemaining
		{
			get { return jobExecutionCountRemaining; }
			set
			{
				if (value.HasValue && value.Value < 0)
					throw new ArgumentOutOfRangeException("value", "The job execution count remaining must not be negative.");

				jobExecutionCountRemaining = value;
			}
		}

		/// <summary>
		/// Gets or sets the amount of time by which the scheduler is permitted to miss
		/// the next scheduled time before a misfire occurs or null if the trigger never misfires.
		/// </summary>
		/// <remarks>
		/// The default is null.
		/// </remarks>
		/// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="value"/> is negative</exception>
		public TimeSpan? MisfireThreshold
		{
			get { return misfireThreshold; }
			set
			{
				if (value.HasValue && value.Value.Ticks < 0)
					throw new ArgumentOutOfRangeException("value", "The misfire threshold must not be negative.");

				misfireThreshold = value;
			}
		}

		/// <summary>
		/// Gets or sets the action to perform when the trigger misses a scheduled recurrence.
		/// </summary>
		/// <remarks>
		/// The default is <see cref="TriggerScheduleAction.Skip"/>.
		/// </remarks>
		public TriggerScheduleAction MisfireAction
		{
			get { return misfireAction; }
			set { misfireAction = value; }
		}

		/// <summary>
		/// Gets or sets whether the next trigger firing should occur at <see cref="StartTimeUtc" />
		/// or at the next recurrence period according to <see cref="Period" />.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This property is initially set to true when the trigger is created so that missing
		/// the time specified by <see cref="StartTimeUtc" /> is considered a misfire.
		/// Once the first time is processed, the property is set to false and the trigger will
		/// skip over as many recurrence periods as needed to catch up with real time.
		/// </para>
		/// <para>
		/// It may be useful to initialize this property to false after creating the trigger
		/// if you do not care whether the trigger fires at the designated start time and
		/// simply wish to ensure that it followed the expected recurrence pattern.  However,
		/// if the <see cref="Period" /> is null, then the trigger will not fire if the time
		/// indicated by <see cref="StartTimeUtc" /> has already passed and will immediately
		/// become inactive.
		/// </para>
		/// </remarks>
		public bool IsFirstTime
		{
			get { return isFirstTime; }
			set { isFirstTime = value; }
		}

		/// <inheritdoc />
		public override DateTime? NextFireTimeUtc
		{
			get { return nextFireTimeUtc; }
		}

		/// <inheritdoc />
		public override TimeSpan? NextMisfireThreshold
		{
			get { return misfireThreshold; }
		}

		/// <inheritdoc />
		public override bool IsActive
		{
			get { return ! jobExecutionCountRemaining.HasValue || jobExecutionCountRemaining.Value > 0; }
		}

		/// <inheritdoc />
		public override Trigger Clone()
		{
			PeriodicTrigger clone = new PeriodicTrigger(startTimeUtc, endTimeUtc, period, jobExecutionCountRemaining);
			clone.nextFireTimeUtc = nextFireTimeUtc;
			clone.misfireThreshold = misfireThreshold;
			clone.misfireAction = misfireAction;
			clone.isFirstTime = isFirstTime;

			return clone;
		}

		/// <inheritdoc />
		public override TriggerScheduleAction Schedule(TriggerScheduleCondition condition, DateTime timeBasisUtc,
		                                               JobExecutionDetails lastJobExecutionDetails)
		{
			timeBasisUtc = DateTimeUtils.AssumeUniversalTime(timeBasisUtc);

			switch (condition)
			{
				case TriggerScheduleCondition.Latch:
					return ScheduleSuggestedAction(TriggerScheduleAction.Skip, timeBasisUtc);

				case TriggerScheduleCondition.Misfire:
					isFirstTime = false;
					return ScheduleSuggestedAction(misfireAction, timeBasisUtc);

				case TriggerScheduleCondition.Fire:
					isFirstTime = false;
					return ScheduleSuggestedAction(TriggerScheduleAction.ExecuteJob, timeBasisUtc);

				default:
					throw new SchedulerException(String.Format(CultureInfo.CurrentCulture,
					                                           "Unrecognized trigger schedule condition '{0}'.", condition));
			}
		}

		internal virtual TriggerScheduleAction ScheduleSuggestedAction(TriggerScheduleAction action, DateTime timeBasisUtc)
		{
			switch (action)
			{
				case TriggerScheduleAction.Stop:
					break;

				case TriggerScheduleAction.ExecuteJob:
					// If the job cannot execute again then stop.
					if (jobExecutionCountRemaining.HasValue && jobExecutionCountRemaining.Value <= 0)
						break;

					// If the end time has passed then stop.
					if (endTimeUtc.HasValue && timeBasisUtc > endTimeUtc.Value)
						break;

					// If the start time is still in the future then hold off until then.
					if (timeBasisUtc < startTimeUtc)
					{
						nextFireTimeUtc = startTimeUtc;
						return TriggerScheduleAction.Skip;
					}

					// Otherwise execute the job.
					nextFireTimeUtc = null;
					jobExecutionCountRemaining -= 1;
					return TriggerScheduleAction.ExecuteJob;

				case TriggerScheduleAction.DeleteJob:
					nextFireTimeUtc = null;
					jobExecutionCountRemaining = 0;
					return TriggerScheduleAction.DeleteJob;

				case TriggerScheduleAction.Skip:
					// If the job cannot execute again then stop.
					if (jobExecutionCountRemaining.HasValue && jobExecutionCountRemaining.Value <= 0)
						break;

					// If the start time is still in the future then hold off until then.
					if (isFirstTime || timeBasisUtc < startTimeUtc)
					{
						nextFireTimeUtc = startTimeUtc;
						return TriggerScheduleAction.Skip;
					}

					// If the trigger is not periodic then we must be skipping the only chance the
					// job had to run so stop the trigger.
					if (! period.HasValue)
						break;

					// Compute when the next occurrence should be.
					TimeSpan timeSinceStart = timeBasisUtc - startTimeUtc;
					TimeSpan timeSinceLastPeriod = new TimeSpan(timeSinceStart.Ticks%period.Value.Ticks);
					nextFireTimeUtc = timeBasisUtc + period - timeSinceLastPeriod;

					// If the next occurrence is past the end time then stop.
					if (nextFireTimeUtc > endTimeUtc)
						break;

					// Otherwise we're good.
					return TriggerScheduleAction.Skip;
			}

			// Stop the trigger.
			nextFireTimeUtc = null;
			jobExecutionCountRemaining = 0;
			return TriggerScheduleAction.Stop;
		}
	}



    /// <summary>
    /// Daily Fire Window
    /// </summary>
    public class DailyFireWindow
    {
        /// <summary>
        /// UTC Start hour for the fire window
        /// </summary>
        public int StartHour { get; set; }

        /// <summary>
        /// UTC End hour for the fire window
        /// </summary>
        public int? EndHour { get; set; }

        /// <summary>
        /// Creates a Daily Fire Window
        /// </summary>
        /// <param name="StartHour">UTC Hour for the window to start</param>
        /// <param name="EndHour">UTC Hour for the window to end</param>
        public DailyFireWindow(int StartHour, int? EndHour)
        {
            this.StartHour = StartHour;
            this.EndHour = EndHour;
        }
    }



    /// <summary>
    /// something goes here...
    /// </summary>
    public class DailyPeriodicWindowTrigger : PeriodicTrigger
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startDateUtc">The UTC date for the trigger to start firing</param>
        /// <param name="endDateUtc">The UTC date for the trigger to stop firing</param>
        /// <param name="dailyFireWindow">Specifies the UTC hours when the trigger should fire</param>
        /// <param name="period">The recurrence period of the trigger.
        /// If the period is set to null, the trigger will fire exactly once
        /// and never recur.</param>
        /// <param name="jobExecutionCount">The number of job executions remaining before the trigger
        /// stops firing.  This number is decremented each time the job executes
        /// until it reaches zero.  If the count is set to null, the number of times the job
        /// may execute is unlimited.</param>
        public DailyPeriodicWindowTrigger(DateTime startDateUtc, DateTime? endDateUtc, DailyFireWindow dailyFireWindow, TimeSpan? period, int? jobExecutionCount)
            : base(startDateUtc, endDateUtc, period, jobExecutionCount)
        {
            DailyFireWindow = dailyFireWindow;
        }

        /// <summary>
        /// The Daily Fire Window for the trigger
        /// </summary>
        public DailyFireWindow DailyFireWindow { get; set; }

        /// <summary>
        /// Makes a deep clone of the trigger
        /// </summary>
        /// <returns></returns>
        public override Trigger Clone()
        {
            var dailyFireWindow = new DailyFireWindow(DailyFireWindow.StartHour, DailyFireWindow.EndHour);
            var clone = new DailyPeriodicWindowTrigger(startTimeUtc, endTimeUtc, dailyFireWindow, period, jobExecutionCountRemaining)
                                                   {
                                                       nextFireTimeUtc = nextFireTimeUtc,
                                                       misfireThreshold = misfireThreshold,
                                                       misfireAction = misfireAction,
                                                       isFirstTime = isFirstTime
                                                   };

            return clone;
        }

        internal override TriggerScheduleAction ScheduleSuggestedAction(TriggerScheduleAction action, DateTime timeBasisUtc)
        {
            if (action != TriggerScheduleAction.ExecuteJob)
                return base.ScheduleSuggestedAction(action, timeBasisUtc);
            
            if (CannotExecuteAgain() || EndTimeHasPassed(timeBasisUtc))
                return StopAction();

            if (!StartTimeHasPassed(timeBasisUtc) || !InDailyFireWindow(timeBasisUtc))
                return SkipAction();

            return ExecuteAction();
        }

        private bool InDailyFireWindow(DateTime timeBasisUtc)
        {
            var startWindow = timeBasisUtc.Date;
            startWindow = startWindow.AddHours(DailyFireWindow.StartHour);

            var endWindow = timeBasisUtc.Date;
            if (DailyFireWindow.EndHour.HasValue)
            {
                if (DailyFireWindow.EndHour >= DailyFireWindow.StartHour)
                    endWindow = endWindow.AddHours(DailyFireWindow.EndHour.Value);
                else
                    endWindow = endWindow.AddDays(1).AddHours(DailyFireWindow.EndHour.Value);
            }

            if (startWindow <= timeBasisUtc &&
                DailyFireWindow.EndHour.HasValue && endWindow > timeBasisUtc)
                return true;

            return false;
        }

        private bool CannotExecuteAgain()
        {
            return jobExecutionCountRemaining.HasValue && jobExecutionCountRemaining.Value <= 0;
        }        

        private bool StartTimeHasPassed(DateTime timeBasisUtc)
        {
            return timeBasisUtc.Date >= startTimeUtc.Date;
        }

        private bool EndTimeHasPassed(DateTime timeBasisUtc)
        {
            return endTimeUtc.HasValue && timeBasisUtc.Date > endTimeUtc.Value.Date;
        }

        private TriggerScheduleAction StopAction()
        {
            nextFireTimeUtc = null;
            jobExecutionCountRemaining = 0;
            return TriggerScheduleAction.Stop;
        }

        private TriggerScheduleAction ExecuteAction()
        {
            nextFireTimeUtc = null;
            jobExecutionCountRemaining -= 1;
            return TriggerScheduleAction.ExecuteJob;
        }

        private TriggerScheduleAction SkipAction()
        {
            nextFireTimeUtc = startTimeUtc;
            return TriggerScheduleAction.Skip;
        }


    }
}