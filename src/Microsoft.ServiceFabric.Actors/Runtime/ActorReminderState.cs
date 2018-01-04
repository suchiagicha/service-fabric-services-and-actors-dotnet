// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Actors.Runtime
{
    using System;
    using System.Threading;

    internal class ActorReminderState : IActorReminderState
    {
        private readonly ActorReminderData reminder;
        private readonly TimeSpan nextDueTime;

        public ActorReminderState(ActorReminderData reminder, TimeSpan currentLogicalTime, ReminderCompletedData reminderCompletedData)
        {
            this.reminder = reminder;

            if (reminderCompletedData != null)
            {
                this.nextDueTime = ComputeRemainingTime(currentLogicalTime, reminderCompletedData.LogicalTime, reminder.Period);
            }
            else
            {
                this.nextDueTime = ComputeRemainingTime(currentLogicalTime, reminder.LogicalCreationTime, reminder.DueTime);
            }
        }

        TimeSpan IActorReminderState.RemainingDueTime => this.nextDueTime;

        string IActorReminder.Name => this.reminder.Name;

        TimeSpan IActorReminder.DueTime => this.reminder.DueTime;

        TimeSpan IActorReminder.Period => this.reminder.Period;

        byte[] IActorReminder.State => this.reminder.State;


        private static TimeSpan ComputeRemainingTime(
            TimeSpan currentLogicalTime,
            TimeSpan createdOrLastCompletedTime,
            TimeSpan dueTimeOrPeriod)
        {
            TimeSpan elapsedTime = TimeSpan.Zero;

            if (currentLogicalTime > createdOrLastCompletedTime)
            {
                elapsedTime = currentLogicalTime - createdOrLastCompletedTime;
            }

            // If reminder has negative DueTime or Period, it is not intended to fire again.
            // Skip computing remaining time.
            if (dueTimeOrPeriod < TimeSpan.Zero)
            {
                return Timeout.InfiniteTimeSpan;
            }

            TimeSpan remainingTime = TimeSpan.Zero;

            if (dueTimeOrPeriod > elapsedTime)
            {
                remainingTime = dueTimeOrPeriod - elapsedTime;
            }

            return remainingTime;
        }
    }
}