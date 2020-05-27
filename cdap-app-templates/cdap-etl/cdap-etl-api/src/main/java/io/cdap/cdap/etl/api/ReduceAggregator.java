/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.api;

import io.cdap.cdap.api.annotation.Beta;

/**
 * Groups all input objects into collections and performs an aggregation on the entire group.
 * Objects that have the same group key are placed into the same group for aggregation.
 * When possible, this interface should be used over the {@link Aggregator} interface because this performs better.
 * An {@link Aggregator} will shuffle all data across the cluster before aggregating, whereas
 * this will aggregate both before and after the shuffle.
 * This reduces the amount of data that needs to be sent over the network, as well as reducing the amount
 * of memory required to perform the aggregation.
 *
 * For example, to aggregate and compute the average for the values, the plugin will first group all the values based
 * on the group key, considering it generates following splits:
 * Split 1: (key1, 1), (key1, 2), (key1, 3), (key2, 4)
 * Split 2: (key1, 2), (key1, 3), (key1, 4), (key2, 4)
 * Split 3: (key1, 3), (key1, 4), (key1, 5), (key2, 4)
 *
 * The reduce function will be called in each split to generate following:
 * Split 1: (key1, sum: 6, count: 3), (key2, sum: 4, count: 1)
 * Split 2: (key1, sum: 9, count: 3), (key2, sum: 4, count: 1)
 * Split 3: (key1, sum: 12, count: 3), (key2, sum: 4, count: 1)
 *
 * Data is then shuffled across the cluster such that records with the same key are handled by the same executor:
 * Split 4: (key1, sum: 6, count: 3), (key1, sum:9, count:3), (key1, sum:12, count:3)
 * Split 5: (key2, sum:4, count:1), (key2, sum:4, count:1), (key2, sum:4, count:1)
 *
 * The reduce function is called again to generate:
 * Split 4: (key1, sum:27, count:9)
 * Split 5: (key2, sum:12, count:3)
 *
 * Finally, the finalize method is called to generate the final output value(s):
 * Split 4: (key1, avg: 3)
 * Split 5: (key2, avg: 3)
 *
 * @param <GROUP_KEY> Type of group key
 * @param <GROUP_VALUE> Type of values to group
 * @param <OUT> Type of output object
 */
@Beta
public interface ReduceAggregator<GROUP_KEY, GROUP_VALUE, OUT> {

  /**
   * Emit the group key(s) for a given input value. If no group key is emitted, the input value
   * is filtered out. If multiple group keys are emitted, the input value will be present in multiple groups.
   *
   * @param groupValue the value to group
   * @param emitter the emitter to emit zero or more group keys for the input
   * @throws Exception if there is some error getting the group
   */
  void groupBy(GROUP_VALUE groupValue, Emitter<GROUP_KEY> emitter) throws Exception;

  /**
   * Reduce the given values to a single value. This method is called before and after grouping of the keys.
   * For example, to compute the sum, the returned value will be the sum of two given values.
   * To compute average, the returned value will contain the sum and count for the two given values.
   *
   * @param value1 the value to reduce
   * @param value2 the value to reduce
   * @return the aggregated value of two given values
   */
  GROUP_VALUE reduce(GROUP_VALUE value1, GROUP_VALUE value2) throws Exception;

  /**
   * Finalize the grouped object for the group key into zero or more output objects.
   * The group value will only contain one value which contains the aggregated stats for
   * all the values, this method can use this stat to compute the desired result, i.e, average, standard deviation
   *
   * @param groupKey the key for the group
   * @param groupValue the group value associated with the group key
   * @param emitter the emitter to emit finalized values for the group
   * @throws Exception if there is some error aggregating
   */
  void finalize(GROUP_KEY groupKey, GROUP_VALUE groupValue, Emitter<OUT> emitter) throws Exception;
}
