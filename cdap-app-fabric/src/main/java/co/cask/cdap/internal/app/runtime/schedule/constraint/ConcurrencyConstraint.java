/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.schedule.constraint;

import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.ProtoConstraint;
import co.cask.cdap.proto.id.ProgramRunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A constraint which dictates an upper bound on the number of concurrent schedule runs.
 */
public class ConcurrencyConstraint extends ProtoConstraint.ConcurrencyConstraint implements CheckableConstraint {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrencyConstraint.class);

  public ConcurrencyConstraint(int maxConcurrency) {
    super(maxConcurrency);
  }

  @Override
  public ConstraintResult check(ProgramSchedule schedule, ConstraintContext context) {
    Map<ProgramRunId, RunRecordMeta> activeRuns = context.getActiveRuns(schedule.getProgramId());
    if (activeRuns.size() >= maxConcurrency) {
      LOG.debug("Skipping run of program {} from schedule {} because there are {} active runs.",
                schedule.getProgramId(), schedule.getName(), activeRuns.size());
      return notSatisfied(context);
    }
    return ConstraintResult.SATISFIED;
  }

  private ConstraintResult notSatisfied(ConstraintContext context) {
    if (!waitUntilMet) {
      return ConstraintResult.NEVER_SATISFIED;
    }
    return new ConstraintResult(ConstraintResult.SatisfiedState.NOT_SATISFIED,
                                context.getCheckTimeMillis() + TimeUnit.SECONDS.toMillis(10));
  }
}
